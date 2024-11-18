"""Command-line interface: read saved describe output, print a reset plan and commands."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from . import __version__
from .commands import CommandOptions, build_commands, earliest_hint, epoch_millis
from .parse import InputError, parse_datetime, parse_describe, parse_duration, parse_offsets_file
from .planner import ResetPlan, ResetSpec, build_plan

EXIT_OK = 0
EXIT_INPUT_ERROR = 2
EXIT_BLOCKED = 3


def _read(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _parse_topic_scope(values: list[str] | None) -> dict[str, set[int] | None] | None:
    if not values:
        return None
    scope: dict[str, set[int] | None] = {}
    for value in values:
        topic, sep, parts = value.partition(":")
        if not sep:
            scope[topic] = None
            continue
        try:
            nums = {int(p) for p in parts.split(",") if p.strip()}
        except ValueError as exc:
            raise InputError(f"--topic {value!r}: partitions must be integers, e.g. orders:0,1") from exc
        scope[topic] = nums
    return scope


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="kafka-consumer-reset-plan",
        description=(
            "Plan a Kafka consumer group offset reset from saved `kafka-consumer-groups --describe` output. "
            "Shows new offsets, messages reprocessed or skipped, blockers, and the exact commands. "
            "Never connects to Kafka."
        ),
    )
    p.add_argument("describe_file", help="saved output of `kafka-consumer-groups --describe --group G` ('-' for stdin)")
    target = p.add_mutually_exclusive_group(required=True)
    target.add_argument("--to-earliest", action="store_true", help="reset to the log start offset")
    target.add_argument("--to-latest", action="store_true", help="reset to the log end offset")
    target.add_argument("--to-current", action="store_true", help="reset to the committed offset")
    target.add_argument("--to-offset", type=int, metavar="N", help="reset every selected partition to offset N")
    target.add_argument("--shift-by", type=int, metavar="N", help="move the committed offset by N (negative = rewind)")
    target.add_argument("--to-datetime", metavar="YYYY-MM-DDThh:mm:ss.sss[Z]", help="reset to the first offset at or after this time")
    target.add_argument("--by-duration", metavar="PnDTnHnMnS", help="reset to the first offset at or after now minus this duration")
    p.add_argument("--topic", action="append", metavar="TOPIC[:P,P]", help="limit to a topic or topic:partitions (repeatable; default: all topics in the input)")
    p.add_argument("--earliest-file", metavar="FILE", help="`kafka-get-offsets --time -2` output (log start offsets)")
    p.add_argument("--offsets-for-times-file", metavar="FILE", help="`kafka-get-offsets --time <epoch-ms>` output; required for --to-datetime/--by-duration")
    p.add_argument("--now", metavar="ISO8601", help="reference time for --by-duration (default: current UTC time)")
    p.add_argument("--bootstrap-server", default="BROKER_HOST:9092", help="value to put in the printed commands (default: the placeholder BROKER_HOST:9092)")
    p.add_argument("--command-config", metavar="FILE", help="client properties file to put in the printed commands")
    p.add_argument("--script-suffix", default=".sh", help="suffix of the Kafka CLI scripts (default .sh; use '' for Confluent Platform packages)")
    p.add_argument("--format", choices=("text", "json"), default="text")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return p


def _spec_from_args(args: argparse.Namespace, warnings: list[str]) -> ResetSpec:
    if args.to_earliest:
        return ResetSpec("to-earliest")
    if args.to_latest:
        return ResetSpec("to-latest")
    if args.to_current:
        return ResetSpec("to-current")
    if args.to_offset is not None:
        if args.to_offset < 0:
            raise InputError("--to-offset must be zero or positive")
        return ResetSpec("to-offset", str(args.to_offset), number=args.to_offset)
    if args.shift_by is not None:
        return ResetSpec("shift-by", str(args.shift_by), number=args.shift_by)
    if args.to_datetime is not None:
        dt, zoned = parse_datetime(args.to_datetime)
        if not zoned:
            warnings.append(
                "--to-datetime has no zone suffix: this plan reads it as UTC, but kafka-consumer-groups may "
                "interpret it in the local timezone of the machine running it. Append Z (UTC) or +hh:mm."
            )
        return ResetSpec("to-datetime", args.to_datetime, target_time=dt)
    duration = parse_duration(args.by_duration)
    if args.now:
        now = datetime.fromisoformat(args.now.replace("Z", "+00:00"))
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
    else:
        now = datetime.now(timezone.utc)
    warnings.append(
        "--by-duration is evaluated when the command runs, so the real offsets move as time passes. "
        f"For a repeatable reset use --to-datetime {(now - duration).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}Z."
    )
    return ResetSpec("by-duration", args.by_duration.upper(), target_time=now - duration)


def _fmt(value: int | None) -> str:
    return "-" if value is None else str(value)


def render_text(plan: ResetPlan, commands: list[tuple[str, str]], hints: list[str]) -> str:
    out: list[str] = []
    label = f"--{plan.spec.kind}" + (f" {plan.spec.value}" if plan.spec.value else "")
    out.append(f"Reset plan for group '{plan.group}' ({label})")
    if plan.spec.target_time is not None:
        out.append(f"Target time: {plan.spec.target_time.isoformat()} (epoch ms {epoch_millis(plan.spec.target_time)})")
    out.append("")
    headers = ["TOPIC", "PARTITION", "LOG-START", "CURRENT", "LOG-END", "NEW-OFFSET", "LAG-BEFORE", "LAG-AFTER", "EFFECT"]
    rows = [
        [
            p.topic,
            str(p.partition),
            _fmt(p.log_start_offset),
            _fmt(p.current_offset),
            str(p.log_end_offset),
            _fmt(p.new_offset),
            _fmt(p.lag_before),
            _fmt(p.lag_after),
            p.effect,
        ]
        for p in plan.partitions
    ]
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    out.append("  ".join(h.ljust(w) for h, w in zip(headers, widths)).rstrip())
    for r in rows:
        out.append("  ".join(c.ljust(w) for c, w in zip(r, widths)).rstrip())
    out.append("")
    out.append(f"Total: {plan.total_reprocess} offsets reprocessed, {plan.total_skip} offsets skipped")
    out.append("(offset counts; compaction and transaction markers can make the record count lower)")

    notes = [(p, n) for p in plan.partitions for n in p.notes]
    if notes:
        out.append("")
        out.append("Notes:")
        for p, n in notes:
            out.append(f"  {p.topic}-{p.partition}: {n}")
    if plan.warnings:
        out.append("")
        out.append("Warnings:")
        out.extend(f"  - {w}" for w in plan.warnings)
        if hints:
            out.append("  Get log start offsets with:")
            out.extend(f"    {h}" for h in hints)
    out.append("")
    if plan.blockers:
        out.append("BLOCKED - the reset will fail until these are fixed:")
        out.extend(f"  - {b}" for b in plan.blockers)
    else:
        out.append("No blockers found in the input. Re-check the group state right before executing.")
    out.append("")
    out.append("Commands (printed only; this tool runs nothing):")
    for desc, cmd in commands:
        out.append(f"  {desc}")
        out.append(f"    {cmd}")
    return "\n".join(out) + "\n"


def render_json(plan: ResetPlan, commands: list[tuple[str, str]]) -> str:
    doc = {
        "group": plan.group,
        "spec": {
            "kind": plan.spec.kind,
            "value": plan.spec.value,
            "target_time": plan.spec.target_time.isoformat() if plan.spec.target_time else None,
        },
        "ok_to_execute": plan.ok_to_execute,
        "blockers": plan.blockers,
        "warnings": plan.warnings,
        "total_reprocess": plan.total_reprocess,
        "total_skip": plan.total_skip,
        "partitions": [
            {
                "topic": p.topic,
                "partition": p.partition,
                "log_start_offset": p.log_start_offset,
                "current_offset": p.current_offset,
                "log_end_offset": p.log_end_offset,
                "new_offset": p.new_offset,
                "lag_before": p.lag_before,
                "lag_after": p.lag_after,
                "reprocess": p.reprocess,
                "skip": p.skip,
                "notes": p.notes,
                "error": p.error,
            }
            for p in plan.partitions
        ],
        "commands": [{"step": d.strip(), "command": c} for d, c in commands],
    }
    return json.dumps(doc, indent=2) + "\n"


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    extra_warnings: list[str] = []
    try:
        spec = _spec_from_args(args, extra_warnings)
        group = parse_describe(_read(args.describe_file))
        earliest = parse_offsets_file(_read(args.earliest_file)) if args.earliest_file else None
        times = None
        if spec.kind in ("to-datetime", "by-duration"):
            if not args.offsets_for_times_file:
                ms = epoch_millis(spec.target_time) if spec.target_time else "EPOCH_MS"
                raise InputError(
                    f"--{spec.kind} needs --offsets-for-times-file. Create it with: "
                    f"kafka-get-offsets{args.script_suffix} --bootstrap-server BROKER_HOST:9092 "
                    f"--topic TOPIC --time {ms} > offsets-for-times.txt"
                )
            times = parse_offsets_file(_read(args.offsets_for_times_file))
        plan = build_plan(
            group,
            spec,
            topics=_parse_topic_scope(args.topic),
            earliest=earliest,
            offsets_for_times=times,
        )
    except (InputError, ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_INPUT_ERROR

    plan.warnings[:0] = extra_warnings
    opts = CommandOptions(
        bootstrap_server=args.bootstrap_server,
        command_config=args.command_config,
        script_suffix=args.script_suffix,
    )
    all_partitions: dict[str, set[int]] = {}
    for r in group.rows:
        all_partitions.setdefault(r.topic, set()).add(r.partition)
    commands = build_commands(plan, opts, all_partitions)
    hints = earliest_hint(opts, list(dict.fromkeys(p.topic for p in plan.partitions))) if earliest is None and spec.kind in ("to-earliest", "to-offset", "shift-by") else []

    if args.format == "json":
        sys.stdout.write(render_json(plan, commands))
    else:
        sys.stdout.write(render_text(plan, commands, hints))
    return EXIT_BLOCKED if plan.blockers else EXIT_OK


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
