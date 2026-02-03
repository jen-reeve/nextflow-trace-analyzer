# nextflow-trace-analyzer
Simple Python script for assessing efficiency of Nextflow pipeline processes based the trace file.

```sh
$ python3 nextflow-trace-analyzer.py --help
usage: nextflow-trace-analyzer.py [-h] [--min-tasks MIN_TASKS] [--out OUT] [--config-out CONFIG_OUT] [--default-executor {slurm,pbs,local}] trace

Nextflow trace efficiency analyzer with concurrency-aware head sizing

positional arguments:
  trace

options:
  -h, --help            show this help message and exit
  --min-tasks MIN_TASKS
  --out OUT
  --config-out CONFIG_OUT
  --default-executor {slurm,pbs,local}
  ```

## Example

Included is the trace file (`trace-example.txt`) from a test execution of the nf-core/sarek pipeline which can be used as an example input trace file.