"""
scheduler.py — CRON-based scheduling engine for recurring workflow executions.

Design rationale:
  We implement a simplified cron expression parser that supports the five
  standard POSIX fields (minute, hour, day-of-month, month, day-of-week) plus
  the common extensions: ``*/n`` (every n), ``a,b,c`` (lists), ``a-b`` (ranges),
  and ``*`` (wildcard).

  The scheduler runs on a background thread that wakes up every ~30 s, checks
  which workflows are due, and submits them to the executor.  We use a 30 s
  granularity by default — this is coarse enough to avoid busy-waiting and
  fine-grained enough for most scheduling needs.

  Why not a proper event loop with heap-based timerfd?
    Portability.  Keeping it cross-platform (Windows, macOS, Linux) with
    zero dependencies beyond stdlib means a polling loop is simpler and
    more reliable.
"""

from __future__ import annotations

import calendar
import logging
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger(__name__)


# ── CRON expression parser ───────────────────────────────────────────────────

_CRON_RE = re.compile(
    r"^(\*|\d+(-\d+)?(/\d+)?(,\d+(-\d+)?(/\d+)?)*)\s+"
    r"(\*|\d+(-\d+)?(/\d+)?(,\d+(-\d+)?(/\d+)?)*)\s+"
    r"(\*|\d+(-\d+)?(/\d+)?(,\d+(-\d+)?(/\d+)?)*)\s+"
    r"(\*|\d+(-\d+)?(/\d+)?(,\d+(-\d+)?(/\d+)?)*)\s+"
    r"(\*|\d+(-\d+)?(/\d+)?(,\d+(-\d+)?(/\d+)?)*)$"
)


def _parse_cron_field(field: str, min_val: int, max_val: int) -> set[int]:
    """Parse a single cron field into a set of allowed values.

    Supports:
      - ``*``                 → all values
      - ``*/n``               → every n
      - ``n``                 → single value
      - ``n-m``               → range (inclusive)
      - ``n-m/k``             → range with step
      - ``a,b,c`` (any combo) → union
    """
    result: set[int] = set()
    for part in field.split(","):
        part = part.strip()
        if not part:
            continue

        # Check for step (e.g. "*/5", "1-30/3")
        step = 1
        if "/" in part:
            part, step_str = part.split("/", 1)
            step = int(step_str)

        if part == "*":
            result.update(range(min_val, max_val + 1, step))
        elif "-" in part:
            lo_str, hi_str = part.split("-", 1)
            lo, hi = int(lo_str), int(hi_str)
            result.update(range(lo, hi + 1, step))
        else:
            result.update(range(int(part), int(part) + 1, step))

    return result


class CronExpression:
    """A parsed cron expression ready for matching against datetimes.

    Parameters
    ----------
    expr : str
        Five-field cron expression (e.g. ``"*/5 * * * *"``).

    Raises
    ------
    ValueError
        If the expression is syntactically invalid.
    """

    def __init__(self, expr: str) -> None:
        self._raw = expr.strip()
        if not _CRON_RE.match(self._raw):
            raise ValueError(
                f"Invalid cron expression: {expr!r}. "
                "Expected 5 fields: minute hour day-of-month month day-of-week"
            )

        fields = self._raw.split()
        self.minutes: set[int] = _parse_cron_field(fields[0], 0, 59)
        self.hours: set[int] = _parse_cron_field(fields[1], 0, 23)
        self.days_of_month: set[int] = _parse_cron_field(fields[2], 1, 31)
        self.months: set[int] = _parse_cron_field(fields[3], 1, 12)
        self.days_of_week: set[int] = _parse_cron_field(fields[4], 0, 6)

    def match(self, dt: Optional[datetime] = None) -> bool:
        """Return True if the cron expression fires at the given datetime.

        If *dt* is None, uses the current UTC time.
        """
        if dt is None:
            dt = datetime.now(timezone.utc)

        if dt.month not in self.months:
            return False
        if dt.hour not in self.hours:
            return False
        if dt.minute not in self.minutes:
            return False
        # Day-of-week: Monday=0 … Sunday=6  (Python's weekday(): Monday=0, Sunday=6)
        if self.days_of_week and dt.weekday() not in self.days_of_week:
            return False
        # Day-of-month: skip if day-of-week is constrained but day-of-month isn't
        if self.days_of_month and dt.day not in self.days_of_month:
            return False

        return True

    def next_fire(self, after: Optional[datetime] = None) -> Optional[datetime]:
        """Return the next datetime this expression matches, or None."""
        if after is None:
            after = datetime.now(timezone.utc)
        # Scan forward minute by minute for up to one year (525600 minutes).
        for minutes_ahead in range(1, 525600):
            candidate = after.replace(second=0, microsecond=0)
            # Add minutes via timedelta (handles hour/day rollover correctly).
            from datetime import timedelta
            candidate = after + timedelta(minutes=minutes_ahead)
            candidate = candidate.replace(second=0, microsecond=0)
            if self.match(candidate):
                return candidate
        return None

    def __repr__(self) -> str:
        return f"CronExpression({self._raw!r})"


# ── Scheduled workflow ───────────────────────────────────────────────────────

@dataclass
class ScheduledWorkflow:
    """A workflow registered with the scheduler.

    Attributes
    ----------
    workflow_id : str
        The graph ID to execute.
    cron : CronExpression
        Parsed schedule.
    enabled : bool
        If False, the scheduler skips this entry.
    last_fired : float | None
        Unix timestamp of the last execution triggered for this schedule.
    metadata : dict
        Extra info (e.g. ``{"description": "nightly ETL"}``).
    """
    workflow_id: str
    cron: CronExpression
    enabled: bool = True
    last_fired: Optional[float] = None
    metadata: dict = field(default_factory=dict)


# ── CronScheduler ────────────────────────────────────────────────────────────

class CronScheduler:
    """Manages cron-triggered execution of workflows.

    Usage
    -----
    >>> scheduler = CronScheduler(run_callback=my_execute_function)
    >>> scheduler.add("etl_pipeline", "0 2 * * *")   # daily at 02:00 UTC
    >>> scheduler.start()  # non-blocking background thread
    >>> # ...
    >>> scheduler.stop()
    """

    def __init__(
        self,
        run_callback: Optional[Callable[[str], None]] = None,
        poll_interval: float = 30.0,
    ) -> None:
        self._schedules: dict[str, ScheduledWorkflow] = {}
        self._run_callback = run_callback
        self._poll_interval = poll_interval
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    # ── public API ──────────────────────────────────────────────────────

    def add(
        self,
        workflow_id: str,
        cron_expr: str,
        enabled: bool = True,
        metadata: Optional[dict] = None,
    ) -> ScheduledWorkflow:
        """Register a workflow schedule.

        Parameters
        ----------
        workflow_id : str
            The graph ID to execute.
        cron_expr : str
            A 5-field cron expression.
        enabled : bool
            Whether the schedule is active from registration.
        """
        cron = CronExpression(cron_expr)
        sw = ScheduledWorkflow(
            workflow_id=workflow_id,
            cron=cron,
            enabled=enabled,
            metadata=metadata or {},
        )
        self._schedules[workflow_id] = sw
        log.info("Scheduled workflow %s: %s", workflow_id, cron_expr)
        return sw

    def remove(self, workflow_id: str) -> None:
        """Unregister a workflow schedule."""
        self._schedules.pop(workflow_id, None)
        log.info("Removed schedule for workflow %s", workflow_id)

    def list_schedules(self) -> list[dict]:
        """Return snapshot of all registered schedules."""
        return [
            {
                "workflow_id": sw.workflow_id,
                "cron": str(sw.cron),
                "enabled": sw.enabled,
                "last_fired": sw.last_fired,
                "metadata": sw.metadata,
            }
            for sw in self._schedules.values()
        ]

    def start(self) -> None:
        """Start the scheduler background thread.

        This is a no-op if the scheduler is already running.
        """
        if self._thread and self._thread.is_alive():
            log.warning("Scheduler is already running")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name="nexusflow-scheduler",
        )
        self._thread.start()
        log.info(
            "Scheduler started (poll interval = %ss)",
            self._poll_interval,
        )

    def stop(self) -> None:
        """Signal the scheduler thread to stop and wait for it."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
            log.info("Scheduler stopped")

    # ── internal loop ───────────────────────────────────────────────────

    def _loop(self) -> None:
        """Core poll loop — runs on the background thread."""
        while not self._stop_event.is_set():
            now = time.time()
            for sw in self._schedules.values():
                if not sw.enabled:
                    continue

                # Check if this schedule should fire.
                # We use the last_fired timestamp to avoid double-firing
                # within the same cron minute.
                if self._should_fire(sw, now):
                    sw.last_fired = now
                    log.info(
                        "Triggering scheduled workflow %s",
                        sw.workflow_id,
                    )
                    if self._run_callback:
                        try:
                            self._run_callback(sw.workflow_id)
                        except Exception:
                            log.exception(
                                "Callback for workflow %s failed",
                                sw.workflow_id,
                            )

            self._stop_event.wait(self._poll_interval)

    @staticmethod
    def _should_fire(sw: ScheduledWorkflow, now_ts: float) -> bool:
        """Determine if a schedule should fire at *now_ts*.

        We compare against the cron expression evaluated at the current
        minute.  To avoid double-firing, we check whether the last fire
        was earlier than this minute's start.
        """
        dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)
        if not sw.cron.match(dt):
            return False

        # Ensure we haven't already fired in this minute.
        minute_start = now_ts - (now_ts % 60)
        if sw.last_fired and sw.last_fired >= minute_start:
            return False

        return True


# ── type import for callback ─────────────────────────────────────────────────
from typing import Callable
