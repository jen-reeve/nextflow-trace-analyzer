import nextflow_trace_analyzer as mod


def test_estimate_peak_concurrency_basic():
    windows = [(0, 10), (5, 15), (20, 30)]
    # Overlap between first two gives peak concurrency 2
    assert mod.estimate_peak_concurrency(windows) == 2

    # Empty windows returns at least 1
    assert mod.estimate_peak_concurrency([]) == 1
