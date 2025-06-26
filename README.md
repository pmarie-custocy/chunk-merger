# Chunk Merger

A tool to merge timescale chunks.

This tool allow listing chunks, mergeable chunks, and eventually merges them.

It is doing it in 2 steps:
1. Merge chunks with the same start_date/end_date
2. Merge chunks with adjacent dates.

## Usage

### Build

```sh
$ go build -o . ./...
$ ./chunk-merger --help
chunk-merger is a tool to merge chunks of timescale tables

Usage:
  chunk-merger [flags]
  chunk-merger [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  help        Help about any command
  list        List chunks
  merge       Merge chunks that belongs the same time period, day or month

Flags:
      --db string             Database name (default "custocy")
  -h, --help                  help for chunk-merger
      --interval-end string   Interval end for chunk selection (default "1 month")
      --table string          Table name (default "object_event")

Use "chunk-merger [command] --help" for more information about a command.
```

By default, `chunk-merger` will connect to localhost using the `postgres` user, no password, no encryption. You can change that using a DSN:

```sh
$ DSN="postgres://custocy_user:xxx@localhost/custocy?sslmode=require" ./chunk-merger list
```

### Listing chunks

```sh
$ DSN="postgres://custocy_user:xxx@localhost/custocy?sslmode=require" ./chunk-merger --db custocy --table object_event list
```

### Merging chunks

```sh
$ DSN="postgres://custocy_user:xxx@localhost/custocy?sslmode=require" ./chunk-merger --db custocy --table object_event merge --type exact --dry-run
```

## Notes

```sql
custocy=# SELECT
  chunk_schema,
  chunk_name,
  range_start,
  range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'object_event'
ORDER BY range_start;
```
