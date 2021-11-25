# kafka-consumer-reset-plan

Plan Kafka consumer group offset resets safely: preview the new offsets and lag, and print the exact kafka-consumer-groups command, never running it.

## Planned scope

Input: `kafka-consumer-groups --describe` output for a group and a reset target (to-earliest, to-latest, to-offset, shift-by, to-datetime with a supplied offsets-for-times file, by-duration). Compute per-partition new offset, messages that will be reprocessed or skipped, warn when the group has active members (reset requires an empty group), when targets fall outside log start/end, and print the exact `kafka-consumer-groups --reset-offsets ... --dry-run` and `--execute` commands. The tool itself never connects to Kafka or runs anything.
