# s3pq

`s3pq` is a simple CLI tool for querying Parquet or Delta tables stored in S3 (via `s3a://`) using PySpark.

It supports:
- **show** → Display the first N rows
- **count** → Count total rows
- **search** → Filter by column=value and show matching rows

## Installation

From PyPI (once published):

```bash
pip install --index-url https://test.pypi.org/simple/ s3pq

pip install s3pq
````

From source:

```bash
git clone https://github.com/paramsehdl/s3pq.git
cd s3pq
pip install .
```

For development (auto-reload changes):

```bash
pip install --editable .
```

## Usage

```bash
s3pq <path> [--delta] <command> [options]
```

### Commands

| Command  | Description                                  |
| -------- | -------------------------------------------- |
| `show`   | Show the first N rows                        |
| `count`  | Count the total rows                         |
| `search` | Filter rows by column=value and show results |

### Options

| Option     | Description                                               |
| ---------- | --------------------------------------------------------- |
| `<path>`   | S3A path to the dataset (e.g., `s3a://my-bucket/data`)    |
| `--delta`  | Treat dataset as a Delta table                            |
| `--limit`  | Number of rows to show (for `show` command, default = 20) |
| `--filter` | Filter in `column=value` format, can be repeated for AND  |

## Examples

**Show first 10 rows from a Delta table**

```bash
s3pq s3a://my-bucket/my-delta-table --delta show --limit 10
```

**Count rows in a Parquet dataset**

```bash
s3pq s3a://my-bucket/my-parquet-data count
```

**Search for ticker="ABC"**

```bash
s3pq s3a://my-bucket/my-delta-table --delta search --filter ticker=ABC
```

**Search with multiple filters**

```bash
s3pq s3a://my-bucket/my-delta-table --delta search --filter ticker=ABC --filter date=2024-08-14
```

## Requirements

* Python 3.8+
* Java installed and on `PATH` (required by PySpark)
* AWS credentials configured in the environment (for S3 access)
* Spark 3.5.x
* Delta Lake 3.3.x

## License

[MIT](LICENSE)