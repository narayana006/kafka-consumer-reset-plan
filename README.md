# kafka-consumer-reset-plan

Plan a Kafka consumer group offset reset before you run it: per-partition new offsets, how many
messages will be reprocessed or skipped, what will block the reset, and the exact
`kafka-consumer-groups` commands to preview, execute and roll back. It reads saved CLI output and
never connects to Kafka or runs anything.

**Status:** v0.1. Working today: parsing saved `kafka-consumer-groups --describe` and
`kafka-get-offsets` output; all reset targets of the real tool except `--from-file`
(`--to-earliest`, `--to-latest`, `--to-current`, `--to-offset`, `--shift-by`, `--to-datetime`,
`--by-duration`); blocker detection; command generation with a rollback step; text and JSON output.
The offset rules were checked against `--reset-offsets --dry-run` on Apache Kafka 3.9.0.

## Install

```bash
pip install git+https://github.com/narayana006/kafka-consumer-reset-plan
```

Python 3.10+, no runtime dependencies.

## Quick start

The order-processor, billing-sync and shipment-tracker snapshots and offset files were captured
from a throwaway local Apache Kafka 3.9.0 broker. The active-member and uncommitted-group files
are illustrative snapshots in the same CLI format. Names and fixed event timestamps are fictional.

```bash
kafka-consumer-reset-plan examples/order-processor.describe.txt --to-earliest \
    --earliest-file examples/earliest-offsets.txt --bootstrap-server broker1.example.com:9092
```

```text
Reset plan for group 'order-processor' (--to-earliest)

TOPIC   PARTITION  LOG-START  CURRENT  LOG-END  NEW-OFFSET  LAG-BEFORE  LAG-AFTER  EFFECT
orders  0          150        419      419      150         0           269        reprocess 269
orders  1          0          281      404      0           123         404        reprocess 281
orders  2          40         0        377      40          377         337        none (moves to log start)

Total: 550 offsets reprocessed, 0 offsets skipped
(offset counts; compaction and transaction markers can make the record count lower)

Notes:
  orders-2: committed offset 0 is below log start 40: 40 records were already removed by retention or delete-records

No blockers found in the input. Re-check the group state right before executing.

Commands (printed only; this tool runs nothing):
  1. Confirm the group is empty (STATE must be Empty, #MEMBERS 0)
    kafka-consumer-groups.sh --bootstrap-server broker1.example.com:9092 --describe --group order-processor --state
  2. Save current offsets for rollback
    kafka-consumer-groups.sh --bootstrap-server broker1.example.com:9092 --group order-processor --reset-offsets --topic orders --to-current --dry-run --export > order-processor-offsets-before-reset.csv
  3. Preview the reset on the cluster (changes nothing)
    kafka-consumer-groups.sh --bootstrap-server broker1.example.com:9092 --group order-processor --reset-offsets --topic orders --to-earliest --dry-run
  4. Execute the reset
    kafka-consumer-groups.sh --bootstrap-server broker1.example.com:9092 --group order-processor --reset-offsets --topic orders --to-earliest --execute
  (rollback) If needed, restore the offsets saved in step 2
    kafka-consumer-groups.sh --bootstrap-server broker1.example.com:9092 --group order-processor --reset-offsets --from-file order-processor-offsets-before-reset.csv --execute
```

The `NEW-OFFSET` column matches what `kafka-consumer-groups --reset-offsets --to-earliest --dry-run`
printed for the same group (150, 0, 40). `orders-2` was committed at 0, but `delete-records` had
already moved its log start to 40, so nothing is really skipped there and the tool says so.

A group with running consumers is flagged, and the exit code is 3:

```bash
kafka-consumer-reset-plan examples/payments-ledger.describe.txt --to-latest
```

```text
Reset plan for group 'payments-ledger' (--to-latest)

TOPIC     PARTITION  LOG-START  CURRENT  LOG-END  NEW-OFFSET  LAG-BEFORE  LAG-AFTER  EFFECT
payments  0          -          400      400      400         0           0          no change
payments  1          -          0        0        0           0           0          no change

Total: 0 offsets reprocessed, 0 offsets skipped
(offset counts; compaction and transaction markers can make the record count lower)

BLOCKED - the reset will fail until these are fixed:
  - group 'payments-ledger' has 2 active member(s) (ledger-1-901261de-693d-4aa2-bf9c-4027c1c28dd9, ledger-2-7f917892-e2a5-4dfd-b3c4-4383db6d70df); Kafka only resets offsets of an inactive group. Stop every consumer in the group first.
```

## Usage

1. Capture the group's offsets (stderr included or not, both work):

   ```bash
   kafka-consumer-groups.sh --bootstrap-server BROKER:9092 --describe --group my-group > my-group.txt 2>&1
   ```

2. Optionally capture log start offsets so `--to-earliest`, `--to-offset` and `--shift-by` can be
   clamped exactly the way the real tool clamps them (without this file the tool assumes 0 and warns):

   ```bash
   kafka-get-offsets.sh --bootstrap-server BROKER:9092 --topic my-topic --time -2 > earliest.txt
   ```

3. Plan:

   ```bash
   kafka-consumer-reset-plan my-group.txt --shift-by -1000 --earliest-file earliest.txt
   ```

Reset targets (exactly one is required; the names match `kafka-consumer-groups`):

| Option | New offset |
|---|---|
| `--to-earliest` | log start offset |
| `--to-latest` | log end offset |
| `--to-current` | committed offset (log end if the partition has none) |
| `--to-offset N` | N, clamped to [log start, log end] |
| `--shift-by N` | committed offset + N, clamped; fails for a partition with no committed offset |
| `--to-datetime T` | first offset at or after T, from `--offsets-for-times-file` |
| `--by-duration PnDTnHnMnS` | first offset at or after now minus the duration, from `--offsets-for-times-file` |

Time-based targets need the broker's answer for that timestamp, which this tool cannot compute
offline. Produce it with `kafka-get-offsets --time <epoch-ms>` (the tool prints the exact command
and the epoch value). Partitions missing from that file have no record at or after the time and
fall back to the log end offset, as the real tool does:

```bash
kafka-consumer-reset-plan examples/shipment-tracker.describe.txt \
    --to-datetime 2025-06-01T10:00:00.000Z \
    --offsets-for-times-file examples/shipments.offsets-for-times.txt
```

Other options:

| Option | Meaning |
|---|---|
| `--topic TOPIC` / `--topic TOPIC:0,2` | limit the plan to topics or partitions (repeatable); the printed commands use the same scope |
| `--now ISO8601` | reference time for `--by-duration` (default: current UTC time) |
| `--bootstrap-server`, `--command-config FILE` | values put into the printed commands |
| `--script-suffix ''` | print `kafka-consumer-groups` instead of `kafka-consumer-groups.sh` (Confluent Platform packaging) |
| `--format json` | machine-readable plan including the commands |

Exit codes: `0` plan has no blockers, `2` bad input or usage, `3` the reset would fail (active
members, rebalancing, or `--shift-by` on a partition with no committed offset).

### What it checks

- **Active members.** Any row with a `CONSUMER-ID` means the group is not empty; Kafka refuses the
  reset ("Assignments can only be reset if the group ... is inactive").
- **Out-of-range targets.** `--to-offset` and `--shift-by` results below log start or above log end
  are clamped, with a note per partition.
- **Committed offset below log start.** Records already removed by retention or `delete-records`
  are reported and not counted as skipped.
- **Uncommitted partitions** (`-` in `CURRENT-OFFSET`): `--shift-by` is blocked; other targets show
  "first commit".
- **Time zones.** A `--to-datetime` without `Z` or `+hh:mm` is read as UTC here and a warning says
  the real tool may use the local time zone of the machine it runs on.

## Feature checklist

- [x] Parse `kafka-consumer-groups --describe` (offsets view) from a file or stdin
- [x] Parse `kafka-get-offsets` output for log start offsets and offsets-for-times
- [x] to-earliest, to-latest, to-current, to-offset, shift-by, to-datetime, by-duration
- [x] Per-partition new offset, lag before/after, offsets reprocessed or skipped
- [x] Blockers: active members, rebalancing, shift-by without a committed offset
- [x] Warnings: out-of-range targets, committed offsets below log start, zone-less datetimes
- [x] Exact `--dry-run`, `--execute`, export-for-rollback and `--from-file` rollback commands, shell-quoted
- [x] JSON output
- [ ] `--from-file` CSV as a reset source
- [ ] Several groups in one input (`--all-groups`)
- [ ] Share groups and streams groups

## Limitations

- It only knows what the saved output says. Offsets move while you read the plan; run the printed
  `--dry-run` command on the cluster right before `--execute`.
- "Reprocessed" and "skipped" are offset differences. Compacted topics and transaction markers mean
  the number of records can be lower.
- Without `--earliest-file` the log start is assumed to be 0.
- `--by-duration` is evaluated by Kafka at execution time, so its result shifts as time passes; the
  tool suggests the equivalent fixed `--to-datetime`.
- Only the default `--describe` (offsets) view is parsed, not `--members` or `--state`.
- The tool never runs any command and has no live Kafka adapter. No permissions are needed to run it.
  On a cluster with ACLs, the printed commands need Describe on the group and topics to describe,
  preview and export, plus Read on the group and topics to execute (offset commits), and the group
  must be empty.

## Contributing

Issues and pull requests are welcome. Run the tests with:

```bash
pip install -e ".[dev]"
pytest
```

If you hit a `--describe` layout the parser rejects, please attach the output (with hostnames
removed) to an issue.

Built and maintained by [Narayana Vemburaj](https://github.com/narayana006).

Licensed under the [MIT License](LICENSE).
