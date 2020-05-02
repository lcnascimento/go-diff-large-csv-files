Go Diff Large Files
----

- Produce large files: `make seeds`
- Diff for tiny files: `make tiny`
- Diff for large files: `make large`
- Run benchmarks: `make benchmarks`

Results of diff should be found at `outputs/<OLD_FILE_NAME>_<NEW_FILE_NAME>/`

Benchmark Summary
----

Seed process produces 10M fake records for 2 files to be "diffed". The produced files have something around 860Mb of size, each.

With this amount of data, my laptop tooked 1m51s to build sqlite tables and 1m46s to query de Diff, using 21Mb of memory.
