"""Parsers for saved kafka-consumer-groups and kafka-get-offsets output."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

DESCRIBE_COLUMNS = (
    "GROUP",
    "TOPIC",
    "PARTITION",
    "CURRENT-OFFSET",
    "LOG-END-OFFSET",
    "LAG",
    "CONSUMER-ID",
    "HOST",
    "CLIENT-ID",
)

_NO_MEMBERS_RE = re.compile(r"Consumer group '(?P<group>[^']+)' has no active members")
_REBALANCING_RE = re.compile(r"Consumer group '(?P<group>[^']+)' is rebalancing")


class InputError(ValueError):
    """Raised when an input file cannot be understood."""


@dataclass(frozen=True)
class PartitionRow:
    group: str
    topic: str
    partition: int
    current_offset: int | None  # None when the describe output shows "-"
    log_end_offset: int
    consumer_id: str | None  # None when "-"
    host: str | None
    client_id: str | None

    @property
    def has_member(self) -> bool:
        return self.consumer_id is not None


@dataclass
class GroupDescription:
    group: str
    rows: list[PartitionRow] = field(default_factory=list)
    reported_no_active_members: bool = False
    reported_rebalancing: bool = False

    @property
    def active_members(self) -> list[str]:
        return sorted({r.consumer_id for r in self.rows if r.consumer_id})

    @property
    def topics(self) -> list[str]:
        seen: list[str] = []
        for r in self.rows:
            if r.topic not in seen:
                seen.append(r.topic)
        return seen


def _dash(value: str) -> str | None:
    return None if value == "-" else value


def parse_describe(text: str) -> GroupDescription:
    """Parse `kafka-consumer-groups --describe --group G` output (offsets view).

    Accepts output captured with or without stderr (the "has no active
    members" notice goes to stderr). Only one group is supported.
    """
    no_members = False
    rebalancing = False
    header_seen = False
    rows: list[PartitionRow] = []
    groups: set[str] = set()

    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        m = _NO_MEMBERS_RE.search(line)
        if m:
            no_members = True
            groups.add(m.group("group"))
            continue
        m = _REBALANCING_RE.search(line)
        if m:
            rebalancing = True
            groups.add(m.group("group"))
            continue
        parts = line.split()
        if parts[:3] == ["GROUP", "TOPIC", "PARTITION"]:
            if "CURRENT-OFFSET" not in parts or "LOG-END-OFFSET" not in parts:
                raise InputError(
                    f"line {lineno}: header is not the offsets view of --describe "
                    "(expected CURRENT-OFFSET and LOG-END-OFFSET columns)"
                )
            header_seen = True
            continue
        if not header_seen:
            continue
        if len(parts) != len(DESCRIBE_COLUMNS):
            raise InputError(
                f"line {lineno}: expected {len(DESCRIBE_COLUMNS)} columns, got {len(parts)}: {line!r}"
            )
        group, topic, partition, current, log_end, _lag, consumer_id, host, client_id = parts
        try:
            current_offset = None if current == "-" else int(current)
            log_end_offset = int(log_end)
            partition_no = int(partition)
        except ValueError as exc:
            raise InputError(f"line {lineno}: non-numeric offset in {line!r}") from exc
        groups.add(group)
        rows.append(
            PartitionRow(
                group=group,
                topic=topic,
                partition=partition_no,
                current_offset=current_offset,
                log_end_offset=log_end_offset,
                consumer_id=_dash(consumer_id),
                host=_dash(host),
                client_id=_dash(client_id),
            )
        )

    if not header_seen:
        raise InputError(
            "no 'GROUP TOPIC PARTITION CURRENT-OFFSET ...' header found; "
            "save the output of: kafka-consumer-groups --describe --group <group>"
        )
    if len(groups) > 1:
        raise InputError(f"input describes more than one group: {', '.join(sorted(groups))}")
    if not rows:
        raise InputError("the describe output has a header but no partition rows")
    rows.sort(key=lambda r: (r.topic, r.partition))
    return GroupDescription(
        group=next(iter(groups)),
        rows=rows,
        reported_no_active_members=no_members,
        reported_rebalancing=rebalancing,
    )


def parse_offsets_file(text: str) -> dict[tuple[str, int], int]:
    """Parse `kafka-get-offsets` output: one `topic:partition:offset` per line.

    Topic names may not contain ':', so splitting from the right is safe.
    Partitions with no result (e.g. no record at or after a timestamp) are
    simply absent from the output.
    """
    offsets: dict[tuple[str, int], int] = {}
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        try:
            topic, partition, offset = line.rsplit(":", 2)
            offsets[(topic, int(partition))] = int(offset)
        except ValueError as exc:
            raise InputError(
                f"offsets file line {lineno}: expected topic:partition:offset, got {line!r}"
            ) from exc
    return offsets


_DURATION_RE = re.compile(
    r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?$"
)


def parse_duration(value: str) -> timedelta:
    """Parse the 'PnDTnHnMnS' form accepted by --by-duration."""
    m = _DURATION_RE.match(value.strip().upper())
    if not m or value.strip().upper() in ("P", "PT"):
        raise InputError(f"duration {value!r} is not in the form PnDTnHnMnS (e.g. PT1H, P1DT6H)")
    parts = {k: float(v) if v else 0.0 for k, v in m.groupdict().items()}
    return timedelta(
        days=parts["days"], hours=parts["hours"], minutes=parts["minutes"], seconds=parts["seconds"]
    )


_DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?(?P<zone>Z|[+-]\d{2}:?\d{2})?$"
)


def parse_datetime(value: str) -> tuple[datetime, bool]:
    """Parse a --to-datetime value. Returns (aware datetime, zone_was_given).

    Without a zone the value is treated as UTC here; the real tool may use
    the local timezone of the machine running it, so callers should warn.
    """
    m = _DATETIME_RE.match(value)
    if not m:
        raise InputError(
            f"datetime {value!r} is not in the form YYYY-MM-DDThh:mm:ss.sss (optionally ending in Z or +hh:mm)"
        )
    zone = m.group("zone")
    text = value
    if zone == "Z":
        text = value[:-1] + "+00:00"
    elif zone and ":" not in zone:
        text = value[: -len(zone)] + zone[:3] + ":" + zone[3:]
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt, zone is not None
