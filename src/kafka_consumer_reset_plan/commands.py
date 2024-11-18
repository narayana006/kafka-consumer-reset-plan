"""Build the exact kafka-consumer-groups / kafka-get-offsets command lines. Never runs them."""

from __future__ import annotations

import shlex
from dataclasses import dataclass
from datetime import datetime

from .planner import ResetPlan, ResetSpec


@dataclass(frozen=True)
class CommandOptions:
    bootstrap_server: str = "BROKER_HOST:9092"
    command_config: str | None = None
    script_suffix: str = ".sh"  # Apache tarball uses .sh; Confluent Platform packages use no suffix

    def script(self, name: str) -> str:
        return f"{name}{self.script_suffix}"


def _join(args: list[str]) -> str:
    return " ".join(shlex.quote(a) for a in args)


def _base(opts: CommandOptions, tool: str) -> list[str]:
    args = [opts.script(tool), "--bootstrap-server", opts.bootstrap_server]
    if opts.command_config:
        args += ["--command-config", opts.command_config]
    return args


def topic_args(plan: ResetPlan, all_partitions: dict[str, set[int]]) -> list[str]:
    """--topic arguments covering exactly the partitions in the plan."""
    selected: dict[str, list[int]] = {}
    for p in plan.partitions:
        selected.setdefault(p.topic, []).append(p.partition)
    args: list[str] = []
    for topic, parts in selected.items():
        if set(parts) == all_partitions.get(topic, set()):
            args += ["--topic", topic]
        else:
            args += ["--topic", f"{topic}:{','.join(str(n) for n in sorted(parts))}"]
    return args


def spec_args(spec: ResetSpec) -> list[str]:
    if spec.kind in ("to-earliest", "to-latest", "to-current"):
        return [f"--{spec.kind}"]
    assert spec.value is not None
    return [f"--{spec.kind}", spec.value]


def topic_regex(topic: str) -> str:
    """kafka-get-offsets treats --topic as a regular expression; escape the only
    metacharacter legal in topic names ('.') so 'a.b' cannot match 'aXb'."""
    return topic.replace(".", "\\.")


def epoch_millis(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def build_commands(plan: ResetPlan, opts: CommandOptions, all_partitions: dict[str, set[int]]) -> list[tuple[str, str]]:
    """Return (description, command) pairs in the order an operator should run them."""
    group = plan.group
    topics = topic_args(plan, all_partitions)
    reset = _base(opts, "kafka-consumer-groups") + ["--group", group, "--reset-offsets"] + topics
    backup_file = f"{group}-offsets-before-reset.csv"
    steps: list[tuple[str, str]] = []

    steps.append((
        "1. Confirm the group is empty (STATE must be Empty, #MEMBERS 0)",
        _join(_base(opts, "kafka-consumer-groups") + ["--describe", "--group", group, "--state"]),
    ))
    if plan.spec.kind in ("to-datetime", "by-duration") and plan.spec.target_time is not None:
        ms = epoch_millis(plan.spec.target_time)
        for topic in dict.fromkeys(p.topic for p in plan.partitions):
            steps.append((
                f"(input) Offsets at {plan.spec.target_time.isoformat()} for {topic}, used as this plan's input",
                _join(_base(opts, "kafka-get-offsets") + ["--topic", topic_regex(topic), "--time", str(ms)]),
            ))
    steps.append((
        "2. Save current offsets for rollback",
        _join(reset + ["--to-current", "--dry-run", "--export"]) + f" > {shlex.quote(backup_file)}",
    ))
    steps.append((
        "3. Preview the reset on the cluster (changes nothing)",
        _join(reset + spec_args(plan.spec) + ["--dry-run"]),
    ))
    steps.append((
        "4. Execute the reset",
        _join(reset + spec_args(plan.spec) + ["--execute"]),
    ))
    steps.append((
        "(rollback) If needed, restore the offsets saved in step 2",
        _join(_base(opts, "kafka-consumer-groups") + ["--group", group, "--reset-offsets", "--from-file", backup_file, "--execute"]),
    ))
    return steps


def earliest_hint(opts: CommandOptions, topics: list[str]) -> list[str]:
    return [
        _join(_base(opts, "kafka-get-offsets") + ["--topic", topic_regex(t), "--time", "-2"]) for t in topics
    ]
