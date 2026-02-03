import nextflow_trace_analyzer as mod
from pathlib import Path
import csv
import tempfile


HEADER = "name\tstatus\texit\tduration\trealtime\tcpus\t%cpu\tmemory\t%mem\trss\tpeak_rss\tworkdir\tnative_id\tsubmit\n"


def make_row(name, realtime, cpus, cpu_pct, memory, rss, vmem, submit):
    return f"{name}\tCOMPLETED\t0\t{realtime}\t{realtime}\t{cpus}\t{cpu_pct}\t{memory}\t0.0%\t0\t{rss}\t/now\t1\t{submit}\n"


def test_analyze_trace_local_recs_and_write(tmp_path):
    p = tmp_path / "trace.txt"
    submit_a = "2020-01-01 00:00:00.000000"
    submit_b = "2020-01-01 00:00:05.000000"

    content = HEADER
    # Two short local tasks for process 'local_proc'
    content += make_row("local_proc (x)", "10s", 2, "150%", "2 GB", "100 MB", "200 MB", submit_a)
    content += make_row("local_proc (x)", "20s", 2, "150%", "2 GB", "100 MB", "200 MB", submit_b)

    # One longer remote task for 'remote_proc'
    content += make_row("remote_proc (y)", "120s", 4, "50%", "4 GB", "500 MB", "600 MB", submit_a)

    p.write_text(content)

    rows, config_map, local_recs, local_windows = mod.analyze_trace(p, min_tasks=1, default_executor=None)

    # rows should contain processed entries for both processes
    names = {r['process'] for r in rows}
    assert 'local_proc' in names
    assert 'remote_proc' in names

    # local_recs should have one entry for local_proc
    assert local_recs and isinstance(local_recs[0], tuple)
    assert len(local_windows) == 2

    # config_map should contain recommendations for local_proc (memory/cpus changes expected)
    assert 'local_proc' in config_map

    # Test write_config produces a file with the process name
    out_cfg = tmp_path / "out.config"
    mod.write_config(config_map, p, out_cfg)
    txt = out_cfg.read_text()
    assert "local_proc" in txt
