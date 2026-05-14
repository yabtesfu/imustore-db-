from imustore import tool


def test_cli_set_get_keys_and_stats(tmp_path, capsys):
    path = tmp_path / "cli.db"

    assert tool.main(["set", str(path), "name", '"Ada"']) == 0
    assert tool.main(["set", str(path), "age", "36"]) == 0
    assert tool.main(["get", str(path), "name"]) == 0
    assert capsys.readouterr().out == '"Ada"\n'

    assert tool.main(["keys", str(path)]) == 0
    assert capsys.readouterr().out.splitlines() == ["age", "name"]

    assert tool.main(["stats", str(path)]) == 0
    assert "record_count" in capsys.readouterr().out


def test_cli_missing_key_returns_code(tmp_path, capsys):
    code = tool.main(["get", str(tmp_path / "missing.db"), "none"])

    assert code == tool.BAD_KEY
    assert "Key not found" in capsys.readouterr().err


def test_cli_scan_and_audit_commands(tmp_path, capsys):
    path = tmp_path / "scan.db"

    assert tool.main(["set", str(path), "user:001", '"Ada"']) == 0
    assert tool.main(["set", str(path), "user:002", '"Grace"']) == 0
    assert tool.main(["set", str(path), "order:001", "42"]) == 0

    assert tool.main(["scan", str(path), "--prefix", "user:"]) == 0
    assert capsys.readouterr().out.splitlines() == [
        '{"key": "user:001", "value": "Ada"}',
        '{"key": "user:002", "value": "Grace"}',
    ]

    assert tool.main(["audit", str(path)]) == 0
    audit_output = capsys.readouterr().out
    assert '"ok": true' in audit_output
    assert '"keys": 3' in audit_output
