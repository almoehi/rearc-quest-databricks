from crawler.state import State


def test_unknown_url_not_current(tmp_path):
    state = State(tmp_path / "state.json")
    state.load()
    assert not state.is_current("https://example.com/f", "2024-01-01")


def test_is_current_after_record(tmp_path):
    state = State(tmp_path / "state.json")
    state.load()
    state.record("https://example.com/f", "2024-01-01")
    assert state.is_current("https://example.com/f", "2024-01-01")


def test_not_current_after_last_modified_change(tmp_path):
    state = State(tmp_path / "state.json")
    state.load()
    state.record("https://example.com/f", "2024-01-01")
    assert not state.is_current("https://example.com/f", "2024-02-01")


def test_none_last_modified_matches(tmp_path):
    state = State(tmp_path / "state.json")
    state.load()
    state.record("https://example.com/f", None)
    assert state.is_current("https://example.com/f", None)
    assert not state.is_current("https://example.com/f", "2024-01-01")


def test_record_upserts(tmp_path):
    state = State(tmp_path / "state.json")
    state.load()
    state.record("https://example.com/f", "2024-01-01")
    state.record("https://example.com/f", "2024-02-01")
    assert state.is_current("https://example.com/f", "2024-02-01")
    assert not state.is_current("https://example.com/f", "2024-01-01")


def test_multiple_urls_independent(tmp_path):
    state = State(tmp_path / "state.json")
    state.load()
    state.record("https://example.com/a", "2024-01-01")
    state.record("https://example.com/b", "2024-02-01")
    assert state.is_current("https://example.com/a", "2024-01-01")
    assert state.is_current("https://example.com/b", "2024-02-01")
    assert not state.is_current("https://example.com/a", "2024-02-01")


def test_persists_across_instances(tmp_path):
    path = tmp_path / "state.json"
    s1 = State(path)
    s1.load()
    s1.record("https://example.com/f", "2024-01-01")

    s2 = State(path)
    s2.load()
    assert s2.is_current("https://example.com/f", "2024-01-01")


def test_atomic_write_leaves_no_tmp(tmp_path):
    state = State(tmp_path / "state.json")
    state.load()
    state.record("https://example.com/f", "2024-01-01")
    assert not (tmp_path / "state.tmp").exists()
