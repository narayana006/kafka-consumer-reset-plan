from datetime import datetime, timezone

import pytest

from kafka_consumer_reset_plan.parse import parse_describe, parse_offsets_file
from kafka_consumer_reset_plan.planner import ResetSpec, build_plan

from .conftest import read

# NEW-OFFSET values printed by `kafka-consumer-groups --reset-offsets --dry-run`
# (Apache Kafka 3.9.0) for the order-processor group in examples/, after
# delete-records moved the log start of orders-0 to 150 and orders-2 to 40.
REAL_DRY_RUN = {
    ResetSpec("to-earliest"): [150, 0, 40],
    ResetSpec("to-latest"): [419, 404, 377],
    ResetSpec("to-current"): [419, 281, 0],
    ResetSpec("shift-by", "-50", number=-50): [369, 231, 40],
    ResetSpec("shift-by", "500", number=500): [419, 404, 377],
    ResetSpec("to-offset", "100", number=100): [150, 100, 100],
    ResetSpec("to-offset", "1000", number=1000): [419, 404, 377],
}


def order_processor():
    return parse_describe(read("order-processor.describe.txt"))


def earliest():
    return parse_offsets_file(read("earliest-offsets.txt"))


@pytest.mark.parametrize("spec,expected", list(REAL_DRY_RUN.items()), ids=lambda v: getattr(v, "kind", ""))
def test_matches_real_kafka_dry_run(spec, expected):
    plan = build_plan(order_processor(), spec, earliest=earliest())
    assert [p.new_offset for p in plan.partitions] == expected
    assert plan.ok_to_execute


def test_to_datetime_matches_real_dry_run():
    times = parse_offsets_file(read("shipments.offsets-for-times.txt"))
    spec = ResetSpec("to-datetime", "2025-06-01T10:00:00.000Z", target_time=datetime(2025, 6, 1, 10, 0, 0, 0, tzinfo=timezone.utc))
    plan = build_plan(parse_describe(read("shipment-tracker.describe.txt")), spec, offsets_for_times=times)
    assert [p.new_offset for p in plan.partitions] == [185, 202, 213]
    assert plan.total_reprocess == 103 + 27 + 90


def test_time_based_falls_back_to_log_end():
    spec = ResetSpec("to-datetime", "2030-01-01T00:00:00.000Z", target_time=datetime(2030, 1, 1, tzinfo=timezone.utc))
    plan = build_plan(order_processor(), spec, offsets_for_times={})
    assert [p.new_offset for p in plan.partitions] == [419, 404, 377]
    assert all("falls back to log end" in p.notes[0] for p in plan.partitions)


def test_time_based_requires_file():
    spec = ResetSpec("by-duration", "PT1H", target_time=datetime(2030, 1, 1, tzinfo=timezone.utc))
    with pytest.raises(ValueError, match="offsets-for-times"):
        build_plan(order_processor(), spec)


def test_reprocess_skip_and_lag():
    plan = build_plan(order_processor(), ResetSpec("to-earliest"), earliest=earliest())
    p0, p1, p2 = plan.partitions
    assert (p0.reprocess, p0.lag_before, p0.lag_after) == (269, 0, 269)
    assert (p1.reprocess, p1.effect) == (281, "reprocess 281")
    # orders-2 is committed at 0 but the log starts at 40: nothing is really skipped
    assert p2.skip == 0 and p2.effect == "none (moves to log start)"
    assert any("below log start" in n for n in p2.notes)


def test_skip_counts_forward_moves():
    plan = build_plan(order_processor(), ResetSpec("to-latest"), earliest=earliest())
    assert [p.skip for p in plan.partitions] == [0, 123, 337]
    assert plan.total_skip == 460


def test_clamp_notes():
    plan = build_plan(order_processor(), ResetSpec("to-offset", "100", number=100), earliest=earliest())
    assert "below log start 150" in plan.partitions[0].notes[0]


def test_unknown_earliest_warns_and_assumes_zero():
    plan = build_plan(order_processor(), ResetSpec("to-earliest"))
    assert [p.new_offset for p in plan.partitions] == [0, 0, 0]
    assert any("log start offsets unknown" in w for w in plan.warnings)


def test_active_members_block():
    plan = build_plan(parse_describe(read("payments-ledger.describe.txt")), ResetSpec("to-earliest"))
    assert not plan.ok_to_execute
    assert "2 active member(s)" in plan.blockers[0]


def test_shift_without_committed_offset_blocks():
    plan = build_plan(parse_describe(read("fraud-scoring.describe.txt")), ResetSpec("shift-by", "-5", number=-5))
    errors = [b for b in plan.blockers if b.startswith("Cannot shift offset")]
    assert len(errors) == 3
    assert all(p.effect == "error" for p in plan.partitions)


def test_to_current_without_commit_uses_log_end():
    plan = build_plan(parse_describe(read("fraud-scoring.describe.txt")), ResetSpec("to-current"))
    assert [p.new_offset for p in plan.partitions] == [288, 309, 303]
    assert all(p.effect == "first commit" for p in plan.partitions)


def test_topic_and_partition_scope():
    group = parse_describe(read("billing-sync.describe.txt"))
    plan = build_plan(group, ResetSpec("to-latest"), topics={"orders": {2}, "payments": None})
    assert [(p.topic, p.partition) for p in plan.partitions] == [("orders", 2), ("payments", 0), ("payments", 1)]


def test_unknown_topic_rejected():
    with pytest.raises(ValueError, match="not in the describe output"):
        build_plan(order_processor(), ResetSpec("to-latest"), topics={"nope": None})


def test_empty_partition_selection_rejected():
    with pytest.raises(ValueError, match="matches no rows"):
        build_plan(order_processor(), ResetSpec("to-latest"), topics={"orders": {9}})
