import nextflow_trace_analyzer as mod


def test_parse_duration():
    assert mod.parse_duration("13m 44s") == 13 * 60 + 44
    assert mod.parse_duration("46.3s") == 46.3
    assert mod.parse_duration("1s") == 1.0
    assert mod.parse_duration(None) is None
    assert mod.parse_duration("no-match") is None


def test_parse_mem():
    assert mod.parse_mem("1.5 GB") == 1.5 * 1024
    assert mod.parse_mem("2.6 MB") == 2.6
    assert mod.parse_mem(None) is None
    assert mod.parse_mem("bad") is None


def test_parse_cpu_pct():
    assert mod.parse_cpu_pct("98.4%") == 98.4
    assert mod.parse_cpu_pct(None) is None
    assert mod.parse_cpu_pct("nope") is None


def test_parse_submit_and_name_and_gb_round():
    ts = "2020-01-02 03:04:05.123456"
    t = mod.parse_submit(ts)
    # timestamp is float; check conversion roughly
    assert int(t) == 1577955845

    assert mod.process_name("PROC (details)") == "PROC"
    assert mod.process_name("NAME") == "NAME"

    assert mod.gb_round(1536) == 2
    assert mod.gb_round(500) == 1
