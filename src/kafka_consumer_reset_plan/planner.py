"""Compute a per-partition reset plan that mirrors kafka-consumer-groups semantics.

Rules reproduced from Apache Kafka's reset tool (checked against 3.9.0):
- --to-offset and --shift-by results outside [log start, log end] are clamped
  to the nearest bound (the tool logs "Value will be set to ...").
- --shift-by fails for a partition with no committed offset.
- --to-datetime / --by-duration use the first offset whose timestamp is at or
  after the target time; partitions with no such record fall back to the log
  end offset ("Falling back to latest known offset").
- Offsets can only be reset while the group has no active members.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from .parse import GroupDescription, PartitionRow

SPECS = ("to-earliest", "to-latest", "to-offset", "shift-by", "to-current", "to-datetime", "by-duration")


@dataclass(frozen=True)
class ResetSpec:
    kind: str  # one of SPECS
    value: str | None = None  # raw value as the user typed it (for the command line)
    number: int | None = None  # offset for to-offset, delta for shift-by
    target_time: datetime | None = None  # resolved time for to-datetime / by-duration

    def __post_init__(self) -> None:
        if self.kind not in SPECS:
            raise ValueError(f"unknown reset spec {self.kind}")


@dataclass
class PartitionPlan:
    topic: str
    partition: int
    current_offset: int | None
    log_start_offset: int | None
    log_end_offset: int
    new_offset: int | None
    notes: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def effective_current(self) -> int | None:
        """Where the consumer would really resume: a committed offset below the
        log start points at deleted records, so reading starts at the log start."""
        if self.current_offset is None:
            return None
        if self.log_start_offset is not None and self.current_offset < self.log_start_offset:
            return self.log_start_offset
        return self.current_offset

    @property
    def delta(self) -> int | None:
        """Offsets moved relative to where the consumer would really resume."""
        if self.new_offset is None or self.effective_current is None:
            return None
        return self.new_offset - self.effective_current

    @property
    def reprocess(self) -> int:
        d = self.delta
        return -d if d is not None and d < 0 else 0

    @property
    def skip(self) -> int:
        d = self.delta
        return d if d is not None and d > 0 else 0

    @property
    def lag_before(self) -> int | None:
        if self.current_offset is None:
            return None
        return self.log_end_offset - self.current_offset

    @property
    def lag_after(self) -> int | None:
        if self.new_offset is None:
            return None
        return self.log_end_offset - self.new_offset

    @property
    def effect(self) -> str:
        if self.error:
            return "error"
        d = self.delta
        if d is None:
            return "first commit"
        if d < 0:
            return f"reprocess {-d}"
        if d > 0:
            return f"skip {d}"
        if self.new_offset != self.current_offset:
            return "none (moves to log start)"
        return "no change"


@dataclass
class ResetPlan:
    group: str
    spec: ResetSpec
    partitions: list[PartitionPlan]
    blockers: list[str]
    warnings: list[str]

    @property
    def total_reprocess(self) -> int:
        return sum(p.reprocess for p in self.partitions)

    @property
    def total_skip(self) -> int:
        return sum(p.skip for p in self.partitions)

    @property
    def ok_to_execute(self) -> bool:
        return not self.blockers


def _clamp(plan: PartitionPlan, wanted: int, start: int) -> int:
    if wanted < start:
        plan.notes.append(f"requested {wanted} is below log start {start}; the tool will use {start}")
        return start
    if wanted > plan.log_end_offset:
        plan.notes.append(
            f"requested {wanted} is above log end {plan.log_end_offset}; the tool will use {plan.log_end_offset}"
        )
        return plan.log_end_offset
    return wanted


def _plan_partition(
    row: PartitionRow,
    spec: ResetSpec,
    earliest: dict[tuple[str, int], int] | None,
    times: dict[tuple[str, int], int] | None,
) -> PartitionPlan:
    key = (row.topic, row.partition)
    start = earliest.get(key) if earliest is not None else None
    plan = PartitionPlan(
        topic=row.topic,
        partition=row.partition,
        current_offset=row.current_offset,
        log_start_offset=start,
        log_end_offset=row.log_end_offset,
        new_offset=None,
    )
    effective_start = start if start is not None else 0
    if earliest is not None and start is None:
        plan.notes.append("partition missing from the earliest-offsets file; log start assumed 0")

    if row.current_offset is not None and start is not None and row.current_offset < start:
        plan.notes.append(
            f"committed offset {row.current_offset} is below log start {start}: "
            f"{start - row.current_offset} records were already removed by retention or delete-records"
        )

    kind = spec.kind
    if kind == "to-earliest":
        plan.new_offset = effective_start
    elif kind == "to-latest":
        plan.new_offset = row.log_end_offset
    elif kind == "to-current":
        if row.current_offset is None:
            plan.new_offset = row.log_end_offset
            plan.notes.append("no committed offset; the tool falls back to the log end offset")
        else:
            plan.new_offset = row.current_offset
    elif kind == "to-offset":
        assert spec.number is not None
        plan.new_offset = _clamp(plan, spec.number, effective_start)
    elif kind == "shift-by":
        assert spec.number is not None
        if row.current_offset is None:
            plan.error = (
                f"Cannot shift offset for partition {row.topic}-{row.partition} "
                "since there is no current committed offset"
            )
        else:
            plan.new_offset = _clamp(plan, row.current_offset + spec.number, effective_start)
    elif kind in ("to-datetime", "by-duration"):
        assert times is not None
        if key in times:
            plan.new_offset = times[key]
        else:
            plan.new_offset = row.log_end_offset
            plan.notes.append("no record at or after the target time; falls back to log end")
    return plan


def build_plan(
    group: GroupDescription,
    spec: ResetSpec,
    *,
    topics: dict[str, set[int] | None] | None = None,
    earliest: dict[tuple[str, int], int] | None = None,
    offsets_for_times: dict[tuple[str, int], int] | None = None,
) -> ResetPlan:
    """Build the plan. `topics` maps topic -> partitions (None = all) to limit scope."""
    if spec.kind in ("to-datetime", "by-duration") and offsets_for_times is None:
        raise ValueError(f"--{spec.kind} needs an offsets-for-times file")

    rows = group.rows
    if topics:
        rows = [r for r in rows if r.topic in topics and (topics[r.topic] is None or r.partition in topics[r.topic])]
        missing = sorted(t for t in topics if t not in group.topics)
        if missing:
            raise ValueError(f"topic(s) not in the describe output: {', '.join(missing)}")
        if not rows:
            raise ValueError("the topic/partition selection matches no rows in the describe output")

    blockers: list[str] = []
    warnings: list[str] = []
    members = group.active_members
    if members:
        blockers.append(
            f"group '{group.group}' has {len(members)} active member(s) ({', '.join(members)}); "
            "Kafka only resets offsets of an inactive group. Stop every consumer in the group first."
        )
    elif group.reported_rebalancing:
        blockers.append(f"group '{group.group}' was rebalancing when described; wait until it is empty and re-check")
    if earliest is None and spec.kind in ("to-earliest", "to-offset", "shift-by"):
        warnings.append(
            "log start offsets unknown (no --earliest-file); assumed 0. Retention may have removed older "
            "records, so the real earliest offset can be higher."
        )

    plans = [_plan_partition(r, spec, earliest, offsets_for_times) for r in rows]
    for p in plans:
        if p.error:
            blockers.append(p.error)
    if any(p.current_offset is None for p in plans):
        warnings.append(
            "some partitions have no committed offset ('-'); reprocess/skip counts are not known for them"
        )
    return ResetPlan(group=group.group, spec=spec, partitions=plans, blockers=blockers, warnings=warnings)
