Go Diff Large Files
----

- Produce large files: `make seeds`
- Diff for tiny files: `make tiny`
- Diff for large files: `make large`
- Run benchmarks: `make benchmarks`

Results of diff should be found at `outputs/<EXECUTION_ID>/`

Benchmark Summary
----

Seed process produces 1000000 fake records to be "diffed". With this amount of data, my laptop tooked 30s to build sqlite tables and 9s to query de Diff, using 19Mb of memory.
