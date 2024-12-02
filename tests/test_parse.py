from datetime import datetime, timedelta, timezone

import pytest

from kafka_consumer_reset_plan.parse import (
    InputError,
    parse_datetime,
    parse_describe,
    parse_duration,
    parse_offsets_file,
)

from .conftest import read


def test_inactive_group_with_stderr_notice():
    g = parse_describe(read("order-processor.describe.txt"))
    assert g.group == "order-processor"
    assert g.reported_no_active_members
    assert g.active_members == []
    assert [(r.topic, r.partition, r.current_offset, r.log_end_offset) for r in g.rows] == [
        ("orders", 0, 419, 419),
        ("orders", 1, 281, 404),
        ("orders", 2, 0, 377),
    ]


def test_active_group_rows_sorted_and_members_found():
    g = parse_describe(read("payments-ledger.describe.txt"))
    assert [r.partition for r in g.rows] == [0, 1]
    assert len(g.active_members) == 2
    assert g.rows[0].client_id == "ledger-1"
    assert g.rows[0].host == "/127.0.0.1"


def test_uncommitted_partitions_are_none():
    g = parse_describe(read("fraud-scoring.describe.txt"))
    assert all(r.current_offset is None for r in g.rows)
    assert g.topics == ["shipments.events"]


def test_multi_topic_group():
    g = parse_describe(read("billing-sync.describe.txt"))
    assert g.topics == ["orders", "payments"]
    assert len(g.rows) == 5


def test_rejects_text_without_header():
    with pytest.raises(InputError, match="no 'GROUP TOPIC"):
        parse_describe("Error: Executing consumer group command failed\n")


def test_rejects_members_view():
    text = "GROUP           CONSUMER-ID  HOST  CLIENT-ID  #PARTITIONS\n"
    with pytest.raises(InputError):
        parse_describe(text)


def test_rejects_state_view_header():
    text = "GROUP TOPIC PARTITION NEW-OFFSET\ng t 0 5\n"
    with pytest.raises(InputError, match="offsets view"):
        parse_describe(text)


def test_rejects_multiple_groups():
    a = read("order-processor.describe.txt")
    b = read("billing-sync.describe.txt")
    with pytest.raises(InputError, match="more than one group"):
        parse_describe(a + b)


def test_rejects_bad_row():
    text = read("order-processor.describe.txt") + "order-processor orders 3 x\n"
    with pytest.raises(InputError, match="columns"):
        parse_describe(text)


def test_rebalancing_notice():
    text = "\nWarning: Consumer group 'g1' is rebalancing.\n\n" + read("order-processor.describe.txt").replace(
        "order-processor", "g1"
    )
    g = parse_describe(text)
    assert g.reported_rebalancing


def test_offsets_file_handles_dotted_topics_and_gaps():
    offsets = parse_offsets_file(read("shipments.offsets-for-times.txt"))
    assert offsets[("shipments.events", 2)] == 213
    assert parse_offsets_file("orders:1:0\n") == {("orders", 1): 0}


def test_offsets_file_bad_line():
    with pytest.raises(InputError, match="topic:partition:offset"):
        parse_offsets_file("orders 1 0\n")


@pytest.mark.parametrize(
    "text,expected",
    [
        ("PT1H", timedelta(hours=1)),
        ("P1DT6H30M", timedelta(days=1, hours=6, minutes=30)),
        ("pt90s", timedelta(seconds=90)),
        ("P2D", timedelta(days=2)),
    ],
)
def test_parse_duration(text, expected):
    assert parse_duration(text) == expected


@pytest.mark.parametrize("bad", ["1h", "PT", "P", "PT1X", "P1W"])
def test_parse_duration_rejects(bad):
    with pytest.raises(InputError):
        parse_duration(bad)


def test_parse_datetime_zones():
    dt, zoned = parse_datetime("2025-06-01T10:00:00.000Z")
    assert zoned and dt == datetime(2025, 6, 1, 10, 0, 0, 0, tzinfo=timezone.utc)
    dt, zoned = parse_datetime("2025-06-01T12:00:00.000+0200")
    assert zoned and dt == datetime(2025, 6, 1, 10, 0, 0, 0, tzinfo=timezone.utc)
    dt, zoned = parse_datetime("2025-11-11T02:46:48")
    assert not zoned and dt.tzinfo == timezone.utc


def test_parse_datetime_rejects():
    with pytest.raises(InputError):
        parse_datetime("11/11/2025")
