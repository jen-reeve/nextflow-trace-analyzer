# nextflow-trace-analyzer
Simple Python script for assessing efficiency of Nextflow pipeline processes based the trace file.

```sh
$ python3 nextflow-trace-analyzer.py --help
usage: nextflow-trace-analyzer.py [-h] --input TRACE [--min-tasks MIN_TASKS] [--out OUT] [--config-out CONFIG_OUT] [--default-executor {slurm,pbs,local}]

Nextflow trace efficiency analyzer

options:
  -h, --help            show this help message and exit
  --input TRACE         Path to Nextflow trace file or directory containing trace files
  --min-tasks MIN_TASKS
                        Minimum number of tasks per process to be considered in evaluation
  --out OUT             Path to output analysis report
  --config-out CONFIG_OUT
                        Path to output Nextflow config file with recommended settings
  --default-executor {slurm,pbs,local}
                        Default executor type for the workflow (used for some specific recommendations)
  ```

## Example

Included is the trace file (`trace-example.txt`) from a test execution of the nf-core/sarek pipeline which can be used as an example input trace file.