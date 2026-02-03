#!/usr/bin/env python3

import argparse
import csv
import re
from statistics import median
from pathlib import Path
import math
from datetime import datetime
import math

# -------------------------
# Regex + constants
# -------------------------

DURATION_RE = re.compile(r"(?:(\d+)m)?\s*(\d+(?:\.\d+)?)s")
MEM_RE = re.compile(r"([\d.]+)\s*(KB|MB|GB|TB)")
CPU_PCT_RE = re.compile(r"([\d.]+)%")

MEM_MULT = {"KB": 1/1024, "MB": 1, "GB": 1024, "TB": 1024 * 1024}
LOCAL_EXECUTOR_THRESHOLD = 30 * 60  # seconds


# -------------------------
# Parsers
# -------------------------

def parse_duration(val):
    if not val:
        return None
    m = DURATION_RE.search(val)
    if not m:
        return None
    minutes = int(m.group(1)) if m.group(1) else 0
    seconds = float(m.group(2))
    return minutes * 60 + seconds


def parse_mem(val):
    if not val:
        return None
    m = MEM_RE.search(val)
    if not m:
        return None
    size, unit = m.groups()
    return float(size) * MEM_MULT[unit]


def parse_cpu_pct(val):
    if not val:
        return None
    m = CPU_PCT_RE.search(val)
    return float(m.group(1)) if m else None


def parse_int(val):
    try:
        return int(val)
    except Exception:
        return None


def parse_submit(ts):
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f").timestamp()


def process_name(raw):
    return raw.split("(")[0].strip()


def gb_round(mb):
    return max(1, round(mb / 1024))


# -------------------------
# Concurrency estimation
# -------------------------

def estimate_peak_concurrency(windows, percentile=0.95):
    """
    windows: list of (start_ts, end_ts)
    """
    events = []
    for s, e in windows:
        events.append((s, +1))
        events.append((e, -1))

    if not events:
        return 1

    events.sort()
    current = 0
    samples = []

    for _, delta in events:
        current += delta
        samples.append(current)

    samples.sort()
    idx = min(len(samples) - 1, math.ceil(len(samples) * percentile) - 1)
    return max(1, samples[idx])


# -------------------------
# Core analysis
# -------------------------

def analyze_trace(trace_file, min_tasks, default_executor):
    data = {}

    traces = (
        [trace_file]
        if trace_file.is_file()
        else [
            f for f in Path(trace_file).iterdir()
            if f.suffix == ".txt" and "trace" in f.name
        ]
    )

    # -------------------------
    # Parse trace(s)
    # -------------------------

    for trace in traces:
        with open(trace, newline="") as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            for row in reader:
                if row.get("status") != "COMPLETED":
                    continue

                name = process_name(row["name"])
                entry = data.setdefault(
                    name,
                    {
                        "durations": [],
                        "cpu_pct": [],
                        "rss": [],
                        "vmem": [],
                        "req_cpus": [],
                        "req_mem": [],
                        "windows": [],
                    },
                )

                d = parse_duration(row.get("realtime"))
                cpu_pct = parse_cpu_pct(row.get("%cpu"))
                rss = parse_mem(row.get("peak_rss"))
                vmem = parse_mem(row.get("peak_vmem"))
                rc = parse_int(row.get("cpus"))
                rm = parse_mem(row.get("memory"))
                submit = row.get("submit")

                if d is not None:
                    entry["durations"].append(d)
                if cpu_pct is not None:
                    entry["cpu_pct"].append(cpu_pct)
                if rss is not None:
                    entry["rss"].append(rss)
                if vmem is not None:
                    entry["vmem"].append(vmem)
                if rc is not None:
                    entry["req_cpus"].append(rc)
                if rm is not None:
                    entry["req_mem"].append(rm)
                if submit and d:
                    s = parse_submit(submit)
                    entry["windows"].append((s, s + d))

    rows = []
    config_map = {}
    local_resource_recs = []
    local_windows = []

    # -------------------------
    # Per-process analysis
    # -------------------------

    for name, vals in data.items():
        ntasks = len(vals["durations"])
        if ntasks < min_tasks:
            continue

        rt_med = median(vals["durations"])
        rt_max = max(vals["durations"])
        cpu_med = median(vals["cpu_pct"]) if vals["cpu_pct"] else None
        rss_max = max(vals["rss"]) if vals["rss"] else None
        vmem_max = max(vals["vmem"]) if vals["vmem"] else None

        cur_cpus = median(vals["req_cpus"]) if vals["req_cpus"] else None
        cur_mem = gb_round(median(vals["req_mem"])) if vals["req_mem"] else None

        recs = []
        cfg = []

        # ---- Local executor ----
        is_local = rt_med < LOCAL_EXECUTOR_THRESHOLD
        if is_local:
            recs.append("Use local executor (short-lived tasks)")
            cfg.append("executor = 'local'")

        # ---- CPU tuning ----
        rec_cpus = None
        if cpu_med:
            est = max(1, round(cpu_med / 100))
            eff = cpu_med / (est * 100)

            if eff < 0.6:
                rec_cpus = max(1, math.floor(cpu_med / 100))
            elif eff > 0.9:
                rec_cpus = est + 1
            else:
                rec_cpus = est

            if cur_cpus and rec_cpus != cur_cpus:
                direction = "Reduce" if rec_cpus < cur_cpus else "Increase"
                recs.append(
                    f"{direction} cpus (current: {cur_cpus} → recommended: {rec_cpus})"
                )
                cfg.append(f"cpus = {rec_cpus}")

        # ---- Memory tuning ----
        rec_mem = None
        if rss_max and vmem_max:
            ratio = rss_max / vmem_max
            if ratio < 0.5:
                rec_mem = gb_round(rss_max * 1.2)
            elif ratio > 0.85:
                rec_mem = gb_round(rss_max * 1.5)
            else:
                rec_mem = gb_round(rss_max)

            if cur_mem and rec_mem != cur_mem:
                direction = "Reduce" if rec_mem < cur_mem else "Increase"
                recs.append(
                    f"{direction} memory (current: {cur_mem} GB → recommended: {rec_mem} GB)"
                )
                cfg.append(f"memory = '{rec_mem} GB'")

        # ---- Runtime variance ----
        if rt_max > 3 * rt_med:
            recs.append("High runtime variance")
            cfg.append("label = 'io_intensive'")

        # ---- SLURM arrays ----
        if (
            default_executor == "slurm"
            and rt_med > LOCAL_EXECUTOR_THRESHOLD
            and ntasks > 100
        ):
            recs.append("Use SLURM job arrays (>100 long-running tasks)")
            cfg.append(f"clusterOptions = '--array=1-{ntasks}'")
            if not any("executor" in x for x in cfg):
                cfg.insert(0, "executor = 'slurm'")
            cfg = [x for x in cfg if x != "executor = 'local'"]

        # ---- Track local requirements ----
        if is_local:
            eff_cpus = rec_cpus if rec_cpus else cur_cpus
            eff_mem  = rec_mem  if rec_mem  else cur_mem

            if eff_cpus and eff_mem:
                local_resource_recs.append((eff_cpus, eff_mem))
                local_windows.extend(vals["windows"])
                
        if cfg:
            config_map[name] = cfg

        rows.append(
            {
                "process": name,
                "tasks": ntasks,
                "runtime_median_min": round(rt_med / 60, 2),
                "recommendations": "; ".join(recs) if recs else "Looks efficient",
            }
        )

    return rows, config_map, local_resource_recs, local_windows


# -------------------------
# Config writer
# -------------------------

def write_config(config_map, trace_file, out_path):
    with open(out_path, "w") as fh:
        fh.write("// Auto-generated Nextflow tuning config\n")
        fh.write(f"// Source trace: {trace_file}\n\n")
        fh.write("process {\n\n")
        for name, lines in sorted(config_map.items()):
            fh.write(f"  withName: '{name}' {{\n")
            for line in lines:
                fh.write(f"    {line}\n")
            fh.write("  }\n\n")
        fh.write("}\n")


# -------------------------
# Main
# -------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Nextflow trace efficiency analyzer with concurrency-aware head sizing"
    )
    ap.add_argument("--input", type=Path, help = "Path to Nextflow trace file or directory containing trace files", dest="trace", required=True)
    ap.add_argument("--min-tasks", type=int, default=1, help="Minimum number of tasks per process to be considered in evaluation", required=False)
    ap.add_argument("--out", type=Path, help="Path to output analysis report", default=None)
    ap.add_argument("--config-out", type=Path, help="Path to output Nextflow config file with recommended settings", default=None)
    ap.add_argument(
        "--default-executor", choices=["slurm", "pbs", "local"], default=None, help="Default executor type for the workflow (used for some specific recommendations)"
    )
    args = ap.parse_args()

    rows, config_map, local_recs, local_windows = analyze_trace(
        args.trace, args.min_tasks, args.default_executor
    )

    if args.out:
        with open(args.out, "w") as fh:
            for r in rows:
                fh.write(f"\n### {r['process']}\n")
                fh.write(f"Tasks: {r['tasks']}\n")
                fh.write(f"Median runtime (min): {r['runtime_median_min']}\n")
                fh.write(f"Recommendations: {r['recommendations']}\n")

            if local_recs:
                max_task_cpu = max(x[0] for x in local_recs)
                max_task_mem = max(x[1] for x in local_recs)
                peak_conc = estimate_peak_concurrency(local_windows)

                fh.write("\n=== Head job resource recommendation (concurrency-aware) ===\n")
                fh.write("Based on observed overlap of local-executor tasks:\n\n")
                fh.write(f"  Estimated peak local concurrency: {peak_conc}\n")
                fh.write(f"  Per-task requirement: {max_task_cpu} cpus, {max_task_mem} GB\n\n")
                fh.write("Recommended head job allocation:\n")
                fh.write(f"  cpus   >= {max_task_cpu * peak_conc}\n")
                fh.write(f"  memory >= {max_task_mem * peak_conc} GB\n")
                fh.write(
                    "\n(Set executor.local.cpus/memory to enforce this limit explicitly.)\n"
                )

    if args.config_out and config_map:
        write_config(config_map, args.trace, args.config_out)


if __name__ == "__main__":
    main()
