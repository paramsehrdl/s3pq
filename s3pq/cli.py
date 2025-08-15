import argparse
from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession, DataFrame


def get_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:3.3.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.driver.memory", "16g")
        .config("spark.executor.memory", "16g")
    )
    return configure_spark_with_delta_pip(
        builder,
        extra_packages=[
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "org.postgresql:postgresql:42.6.0",
        ],
    ).getOrCreate()


def load_df(spark: SparkSession, path: str, delta: bool) -> DataFrame:
    if not path.startswith("s3a://"):
        raise ValueError(f"Path {path} is not a valid S3A path.")

    if not delta:
        return spark.read.parquet(path)

    if not DeltaTable.isDeltaTable(spark, path):
        raise ValueError(f"Path {path} is not a Delta table.")

    return spark.read.format("delta").load(path)


def main():
    parser = argparse.ArgumentParser(description="Process S3 Parquet/Delta queries")
    parser.add_argument("path", help="S3A path to process")
    parser.add_argument("--delta", action="store_true", help="Enable delta mode")
    parser.add_argument(
        "command", choices=["show", "count", "search"], help="Action to perform"
    )
    parser.add_argument("--limit", type=int, default=20, help="Number of rows for show")
    parser.add_argument(
        "--filter",
        action="append",
        help="Filter in form column=value (can be used multiple times for AND)",
    )

    args = parser.parse_args()

    print(f"Path: {args.path}")
    print(f"Delta mode: {args.delta}")
    print(f"Command: {args.command}")

    spark = get_spark_session()
    df = load_df(spark, args.path, args.delta)

    # Apply filters if provided
    if args.filter:
        for cond in args.filter:
            if "=" not in cond:
                raise ValueError(f"Invalid filter format: {cond}")
            col, val = cond.split("=", 1)
            df = df.filter(f"{col} = '{val}'")

    if args.command == "show":
        df.show(args.limit, truncate=False)
    elif args.command == "count":
        print(f"Count: {df.count()}")
    elif args.command == "search":
        df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
