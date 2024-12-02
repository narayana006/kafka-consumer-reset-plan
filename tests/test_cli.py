import json

from kafka_consumer_reset_plan.cli import EXIT_BLOCKED, EXIT_INPUT_ERROR, EXIT_OK, main


def run(capsys, *args):
    code = main(list(args))
    out, err = capsys.readouterr()
    return code, out, err


def test_text_plan_and_commands(capsys, examples):
    code, out, _ = run(
        capsys,
        str(examples / "order-processor.describe.txt"),
        "--to-earliest",
        "--earliest-file", str(examples / "earliest-offsets.txt"),
        "--bootstrap-server", "broker1.example.com:9092",
    )
    assert code == EXIT_OK
    assert "orders  1          0          281      404      0" in out
    assert "Total: 550 offsets reprocessed, 0 offsets skipped" in out
    assert (
        "kafka-consumer-groups.sh --bootstrap-server broker1.example.com:9092 --group order-processor "
        "--reset-offsets --topic orders --to-earliest --execute"
    ) in out
    assert "--to-current --dry-run --export > order-processor-offsets-before-reset.csv" in out
    assert "--from-file order-processor-offsets-before-reset.csv --execute" in out


def test_blocked_exit_code(capsys, examples):
    code, out, _ = run(capsys, str(examples / "payments-ledger.describe.txt"), "--to-latest")
    assert code == EXIT_BLOCKED
    assert "BLOCKED" in out


def test_json_output(capsys, examples):
    code, out, _ = run(
        capsys,
        str(examples / "shipment-tracker.describe.txt"),
        "--by-duration", "PT2H",
        "--now", "2025-06-01T12:00:00.000Z",
        "--offsets-for-times-file", str(examples / "shipments.offsets-for-times.txt"),
        "--format", "json",
    )
    assert code == EXIT_OK
    doc = json.loads(out)
    assert doc["spec"]["target_time"] == "2025-06-01T10:00:00+00:00"
    assert [p["new_offset"] for p in doc["partitions"]] == [185, 202, 213]
    assert any("--to-datetime 2025-06-01T10:00:00.000Z" in w for w in doc["warnings"])
    cmds = [c["command"] for c in doc["commands"]]
    assert any("kafka-get-offsets.sh" in c and "'shipments\\.events'" in c and "1748772000000" in c for c in cmds)
    assert any(c.endswith("--by-duration PT2H --execute") for c in cmds)


def test_missing_times_file_is_input_error(capsys, examples):
    code, _, err = run(capsys, str(examples / "shipment-tracker.describe.txt"), "--to-datetime", "2025-11-11T02:00:00Z")
    assert code == EXIT_INPUT_ERROR
    assert "needs --offsets-for-times-file" in err


def test_zoneless_datetime_warns(capsys, examples):
    code, out, _ = run(
        capsys,
        str(examples / "shipment-tracker.describe.txt"),
        "--to-datetime", "2025-06-01T10:00:00.000",
        "--offsets-for-times-file", str(examples / "shipments.offsets-for-times.txt"),
    )
    assert code == EXIT_OK
    assert "no zone suffix" in out


def test_stdin_and_confluent_names(capsys, monkeypatch, examples):
    import io

    monkeypatch.setattr("sys.stdin", io.StringIO((examples / "order-processor.describe.txt").read_text()))
    code, out, _ = run(capsys, "-", "--shift-by", "-10", "--script-suffix", "", "--command-config", "client.properties")
    assert code == EXIT_OK
    assert "kafka-consumer-groups --bootstrap-server BROKER_HOST:9092 --command-config client.properties" in out
    assert "kafka-get-offsets --bootstrap-server" in out


def test_partition_subset_command(capsys, examples):
    code, out, _ = run(capsys, str(examples / "order-processor.describe.txt"), "--to-offset", "100", "--topic", "orders:0,1")
    assert code == EXIT_OK
    assert "--topic orders:0,1 --to-offset 100 --dry-run" in out
    assert "orders  2" not in out


def test_bad_file(capsys, tmp_path):
    f = tmp_path / "x.txt"
    f.write_text("nothing here\n")
    code, _, err = run(capsys, str(f), "--to-latest")
    assert code == EXIT_INPUT_ERROR
    assert "error:" in err


def test_group_name_is_shell_quoted(capsys, tmp_path, examples):
    text = (examples / "order-processor.describe.txt").read_text().replace("order-processor", "team$a")
    f = tmp_path / "g.txt"
    f.write_text(text)
    code, out, _ = run(capsys, str(f), "--to-latest")
    assert code == EXIT_OK
    assert "--group 'team$a'" in out
