"""Task store for shared team task management."""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from clawteam.team.models import TaskItem, TaskPriority, TaskStatus, get_data_dir


class TaskLockError(Exception):
    """Raised when a task is locked by another agent."""


class TaskCycleError(Exception):
    """Raised when a task cycle is detected."""


def _tasks_root(team_name: str) -> Path:
    d = get_data_dir() / "tasks" / team_name
    d.mkdir(parents=True, exist_ok=True)
    return d


def _task_path(team_name: str, task_id: str) -> Path:
    return _tasks_root(team_name) / f"task-{task_id}.json"


def _tasks_lock_path(team_name: str) -> Path:
    return _tasks_root(team_name) / ".tasks.lock"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TaskStore:
    """File-based task store with dependency tracking.

    Each task is stored as a separate JSON file:
    ``{data_dir}/tasks/{team}/task-{id}.json``
    """

    def __init__(self, team_name: str):
        self.team_name = team_name

    @contextmanager
    def _write_lock(self):
        lock_path = _tasks_lock_path(self.team_name)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def create(
        self,
        subject: str,
        description: str = "",
        owner: str = "",
        priority: TaskPriority | None = None,
        blocks: list[str] | None = None,
        blocked_by: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TaskItem:
        task = TaskItem(
            subject=subject,
            description=description,
            owner=owner,
            priority=priority or TaskPriority.medium,
            blocks=blocks or [],
            blocked_by=blocked_by or [],
            metadata=metadata or {},
        )
        self._validate_blocked_by_unlocked(task.id, task.blocked_by)
        if task.blocked_by:
            task.status = TaskStatus.blocked
        with self._write_lock():
            # Validate dependencies exist
            for dep_id in (blocked_by or []):
                if not self._get_unlocked(dep_id):
                    raise ValueError(f"Task '{dep_id}' does not exist")

            # Validate blocks references exist
            for dep_id in (blocks or []):
                if not self._get_unlocked(dep_id):
                    raise ValueError(f"Task '{dep_id}' does not exist")

            # Check for cycles
            if self._would_create_cycle(task.id, blocked_by or []):
                raise TaskCycleError("Creating this dependency would cause a cycle")

            self._save_unlocked(task)
        return task

    def get(self, task_id: str) -> TaskItem | None:
        return self._get_unlocked(task_id)

    def _get_unlocked(self, task_id: str) -> TaskItem | None:
        path = _task_path(self.team_name, task_id)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return TaskItem.model_validate(data)
        except Exception:
            return None

    def update(
        self,
        task_id: str,
        status: TaskStatus | None = None,
        owner: str | None = None,
        subject: str | None = None,
        description: str | None = None,
        priority: TaskPriority | None = None,
        add_blocks: list[str] | None = None,
        add_blocked_by: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        caller: str = "",
        force: bool = False,
    ) -> TaskItem | None:
        with self._write_lock():
            task = self._get_unlocked(task_id)
            if not task:
                return None

            # Lock logic when transitioning to in_progress
            if status == TaskStatus.in_progress:
                self._acquire_lock(task, caller, force)
                # Record when work actually started
                if not task.started_at:
                    task.started_at = _now_iso()

            # Clear lock when transitioning to completed or pending
            if status in (TaskStatus.completed, TaskStatus.pending):
                task.locked_by = ""
                task.locked_at = ""

            # Compute duration when completing a task that has a start time
            if status == TaskStatus.completed and task.started_at:
                try:
                    start = datetime.fromisoformat(task.started_at)
                    duration_secs = (datetime.now(timezone.utc) - start).total_seconds()
                    task.metadata["duration_seconds"] = round(duration_secs, 2)
                except (ValueError, TypeError):
                    pass  # malformed timestamp, skip

            if status is not None:
                task.status = status
            if owner is not None:
                task.owner = owner
            if subject is not None:
                task.subject = subject
            if description is not None:
                task.description = description
            if priority is not None:
                task.priority = priority
            if add_blocks:
                # Validate referenced tasks exist
                for dep_id in add_blocks:
                    if not self._get_unlocked(dep_id):
                        raise ValueError(f"Task '{dep_id}' does not exist")
                for b in add_blocks:
                    if b not in task.blocks:
                        task.blocks.append(b)
            if add_blocked_by:
                # Validate referenced tasks exist
                for dep_id in add_blocked_by:
                    if not self._get_unlocked(dep_id):
                        raise ValueError(f"Task '{dep_id}' does not exist")

                # Check for self-reference
                if task_id in add_blocked_by:
                    raise ValueError("Task cannot block itself")

                # Check for cycles
                new_blocked_by = task.blocked_by + add_blocked_by
                if self._would_create_cycle(task_id, new_blocked_by):
                    raise TaskCycleError("Adding dependency would create a cycle")

                proposed_blocked_by = list(task.blocked_by)
                for b in add_blocked_by:
                    if b not in proposed_blocked_by:
                        proposed_blocked_by.append(b)
                self._validate_blocked_by_unlocked(task.id, proposed_blocked_by)
                task.blocked_by = proposed_blocked_by
                if task.blocked_by and task.status == TaskStatus.pending:
                    task.status = TaskStatus.blocked
            if metadata:
                task.metadata.update(metadata)
            task.updated_at = _now_iso()

            if task.status == TaskStatus.completed:
                self._resolve_dependents_unlocked(task_id)

            self._save_unlocked(task)
            return task

    def _acquire_lock(self, task: TaskItem, caller: str, force: bool) -> None:
        """Acquire lock on a task for the caller agent."""
        if task.locked_by and task.locked_by != caller and not force:
            # Check if lock holder is still alive via spawn registry
            from clawteam.spawn.registry import is_agent_alive
            alive = is_agent_alive(self.team_name, task.locked_by)
            if alive is not False:
                # Lock holder is alive or unknown — refuse
                raise TaskLockError(
                    f"Task '{task.id}' is locked by '{task.locked_by}' "
                    f"(since {task.locked_at}). Use --force to override."
                )
            # Lock holder is dead — release and continue

        task.locked_by = caller or ""
        task.locked_at = _now_iso() if caller else ""

    def release_stale_locks(self) -> list[str]:
        """Scan all tasks and release locks held by dead agents.

        Returns list of task IDs whose locks were released.
        """
        from clawteam.spawn.registry import is_agent_alive

        released = []
        with self._write_lock():
            for task in self._list_tasks_unlocked():
                if not task.locked_by:
                    continue
                alive = is_agent_alive(self.team_name, task.locked_by)
                if alive is False:
                    task.locked_by = ""
                    task.locked_at = ""
                    task.updated_at = _now_iso()
                    self._save_unlocked(task)
                    released.append(task.id)
        return released

    def list_tasks(
        self,
        status: TaskStatus | None = None,
        owner: str | None = None,
        priority: TaskPriority | None = None,
        sort_by_priority: bool = False,
    ) -> list[TaskItem]:
        return self._list_tasks_unlocked(
            status=status,
            owner=owner,
            priority=priority,
            sort_by_priority=sort_by_priority,
        )

    def _list_tasks_unlocked(
        self,
        status: TaskStatus | None = None,
        owner: str | None = None,
        priority: TaskPriority | None = None,
        sort_by_priority: bool = False,
    ) -> list[TaskItem]:
        root = _tasks_root(self.team_name)
        tasks = []
        for f in sorted(root.glob("task-*.json")):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                task = TaskItem.model_validate(data)
                if status and task.status != status:
                    continue
                if owner and task.owner != owner:
                    continue
                if priority and task.priority != priority:
                    continue
                tasks.append(task)
            except Exception:
                continue
        if sort_by_priority:
            priority_order = {
                TaskPriority.urgent: 0,
                TaskPriority.high: 1,
                TaskPriority.medium: 2,
                TaskPriority.low: 3,
            }
            tasks.sort(key=lambda task: (priority_order.get(task.priority, 2), task.created_at, task.id))
        return tasks

    def get_stats(self) -> dict[str, Any]:
        """Aggregate task timing stats for this team.

        Returns dict with total tasks, completed count, and avg duration
        (only counting tasks that have duration_seconds in metadata).
        """
        tasks = self.list_tasks()
        completed = [t for t in tasks if t.status == TaskStatus.completed]
        durations = [
            t.metadata["duration_seconds"]
            for t in completed
            if "duration_seconds" in t.metadata
        ]
        avg_duration = sum(durations) / len(durations) if durations else 0.0
        return {
            "total": len(tasks),
            "completed": len(completed),
            "in_progress": sum(1 for t in tasks if t.status == TaskStatus.in_progress),
            "pending": sum(1 for t in tasks if t.status == TaskStatus.pending),
            "blocked": sum(1 for t in tasks if t.status == TaskStatus.blocked),
            "timed_completed": len(durations),
            "avg_duration_seconds": round(avg_duration, 2),
        }

    def _validate_blocked_by_unlocked(self, task_id: str, blocked_by: list[str]) -> None:
        if task_id in blocked_by:
            raise ValueError(f"Task '{task_id}' cannot be blocked by itself")

        graph: dict[str, list[str]] = {
            task.id: list(task.blocked_by)
            for task in self._list_tasks_unlocked()
        }
        graph[task_id] = list(blocked_by)

        visiting: set[str] = set()
        visited: set[str] = set()

        def _visit(node: str) -> bool:
            if node in visiting:
                return True
            if node in visited:
                return False
            visiting.add(node)
            for dep in graph.get(node, []):
                if dep in graph and _visit(dep):
                    return True
            visiting.remove(node)
            visited.add(node)
            return False

        for node in graph:
            if _visit(node):
                raise ValueError("Task dependencies cannot contain cycles")

    def _save_unlocked(self, task: TaskItem) -> None:
        path = _task_path(self.team_name, task.id)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            dir=path.parent,
            prefix=f"{path.stem}-",
            suffix=".tmp",
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as tmp_file:
                tmp_file.write(task.model_dump_json(indent=2, by_alias=True))
            Path(tmp_name).replace(path)
        except BaseException:
            Path(tmp_name).unlink(missing_ok=True)
            raise

    def _resolve_dependents_unlocked(self, completed_task_id: str) -> None:
        root = _tasks_root(self.team_name)
        for f in root.glob("task-*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                task = TaskItem.model_validate(data)
                if completed_task_id in task.blocked_by:
                    task.blocked_by.remove(completed_task_id)
                    if not task.blocked_by and task.status == TaskStatus.blocked:
                        task.status = TaskStatus.pending
                    task.updated_at = _now_iso()
                    self._save_unlocked(task)
            except Exception:
                continue

    def has_cycles(self) -> bool:
        """Check if task dependency graph has cycles using Kahn's algorithm.

        Returns True if the graph contains cycles, False otherwise.
        """
        return len(self.detect_cycles()) > 0

    def detect_cycles(self) -> list[list[str]]:
        """Find all cycles in the task dependency graph.

        Returns a list of cycles, where each cycle is a list of task IDs
        forming a circular dependency (e.g., [['a', 'b', 'a']]).
        Uses DFS-based cycle detection.
        """
        tasks = self._list_tasks_unlocked()
        if not tasks:
            return []

        # Build adjacency list: task -> tasks it blocks
        # blocked_by means: this task is blocked by others
        # So we have an edge: blocker -> blocked
        graph: dict[str, set[str]] = {t.id: set() for t in tasks}
        in_degree: dict[str, int] = {t.id: 0 for t in tasks}

        for task in tasks:
            for blocked_by_id in task.blocked_by:
                if blocked_by_id in graph:
                    # Edge from blocked_by_id -> task.id
                    graph[blocked_by_id].add(task.id)
                    in_degree[task.id] = in_degree.get(task.id, 0) + 1

        # Kahn's algorithm to detect cycles
        # Nodes with no incoming edges
        queue = [node for node in in_degree if in_degree[node] == 0]
        visited_count = 0

        while queue:
            node = queue.pop(0)
            visited_count += 1
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # If not all nodes were visited, there are cycles
        if visited_count < len(graph):
            # Find nodes involved in cycles using DFS
            cycles = []
            visited: set[str] = set()
            path: list[str] = []

            def dfs(node: str) -> None:
                if node in path:
                    # Found a cycle
                    cycle_start = path.index(node)
                    cycle = path[cycle_start:] + [node]
                    cycles.append(cycle)
                    return
                if node in visited:
                    return

                visited.add(node)
                path.append(node)

                for neighbor in graph.get(node, []):
                    dfs(neighbor)

                path.pop()

            for task in tasks:
                if task.id not in visited:
                    dfs(task.id)

            return cycles

        return []

    def _would_create_cycle(self, task_id: str, new_blocked_by: list[str]) -> bool:
        """Check if adding blocked_by dependencies would create a cycle.

        Args:
            task_id: The task that would be blocked
            new_blocked_by: List of task IDs that would block this task

        Returns:
            True if adding these dependencies would create a cycle
        """
        if not new_blocked_by:
            return False

        # Check if any of the blocking tasks is the task itself
        if task_id in new_blocked_by:
            return True

        # Build a temporary graph with the proposed edges
        tasks = self._list_tasks_unlocked()
        graph: dict[str, set[str]] = {t.id: set() for t in tasks}
        graph[task_id] = set()  # Ensure task_id is in graph

        # Add existing edges
        for task in tasks:
            for blocked_by_id in task.blocked_by:
                if blocked_by_id in graph:
                    graph[blocked_by_id].add(task.id)

        # Add proposed new edges
        for blocker_id in new_blocked_by:
            if blocker_id in graph:
                graph[blocker_id].add(task_id)

        # Detect cycles using DFS from task_id
        visited: set[str] = set()
        rec_stack: set[str] = set()

        def has_cycle_from(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)

            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if has_cycle_from(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True

            rec_stack.remove(node)
            return False

        # Check if following edges from any of the new blockers leads back to task_id
        for blocker_id in new_blocked_by:
            if blocker_id in graph:
                # Clear visited for each starting point
                visited.clear()
                rec_stack.clear()
                if has_cycle_from(blocker_id):
                    return True

        return False
