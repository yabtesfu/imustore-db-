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
