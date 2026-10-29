"""Build the daily call-center summary using AWS Glue Data Catalog metadata."""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import sys
from datetime import date
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlparse

# CDE images may lack boto3; fall back to a bundled zip attached as a job resource.
try:
    import boto3
except ModuleNotFoundError:
    dependency_archives = (
        Path(__file__).with_name("callcenter_python_deps.zip"),
        Path("/app/mount/callcenter_python_deps.zip"),
    )
    dependency_archive = next(
        (path for path in dependency_archives if path.is_file()),
        None,
    )
    if dependency_archive is None:
        raise RuntimeError(
            "boto3 is unavailable; attach callcenter_python_deps.zip "
            "to the CDE job as a file resource"
        )
    dependency_directory = Path("/tmp/callcenter_python_deps")
    if dependency_directory.exists():
        shutil.rmtree(dependency_directory)
    shutil.unpack_archive(str(dependency_archive), str(dependency_directory), "zip")
    sys.path.insert(0, str(dependency_directory))
    import boto3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_DATABASE = "callcenter_data"
DEFAULT_SOURCE_TABLE = "callcenter_interaction"
DEFAULT_SUMMARY_TABLE = "callcenter_interaction_summary"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a call-center summary from a table registered in AWS Glue."
    )
    parser.add_argument(
        "interaction_date",
        nargs="?",
        default=None,
        help="Optional interaction date in YYYY-MM-DD format. If omitted, INTERACTION_DATE is used.",
    )
    parser.add_argument(
        "--glue-database", default=None,
        help="Glue database containing the source table. Defaults to callcenter_data_s3 for this S3 workflow; pass callcenter_data to use the existing Ozone workflow.",
    )
    parser.add_argument("--source-table", default=DEFAULT_SOURCE_TABLE,
                        help="Glue source table used when --source-s3-uri is not supplied.")
    parser.add_argument(
        "--source-s3-uri", default=None,
        help="Direct S3 source prefix (e.g. s3://ccf-psecoe-ccf-artifacts/s3_flow_data/). "
             "When set, the source location is taken from this URI instead of the Glue source table.",
    )
    parser.add_argument("--summary-table", default=DEFAULT_SUMMARY_TABLE)
    parser.add_argument(
        "--ozone-bucket",
        default="hive-warehouse",
        help="Bucket served by CDE Ozone; its injected S3A settings are preserved.",
    )
    parser.add_argument(
        "--ozone-endpoint",
        default=os.environ.get("OZONE_S3_ENDPOINT"),
        help="Optional S3 gateway URL for an external Ozone cluster.",
    )
    parser.add_argument("--ozone-region", default="us-east-1")
    parser.add_argument(
        "--ozone-truststore",
        default=os.environ.get(
            "OZONE_TRUSTSTORE", "/app/mount/ozone-truststore.jks"
        ),
    )
    parser.add_argument(
        "--output-s3-uri",
        default=None,
        help="Summary root URI. Defaults to a sibling of the source table location.",
    )
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION", "us-west-1"))
    parser.add_argument("--glue-catalog-id", default=None)
    parser.add_argument(
        "--aws-access-key-env",
        default="CALLCENTER_AWS_ACCESS_KEY_ID",
    )
    parser.add_argument(
        "--aws-secret-key-env",
        default="CALLCENTER_AWS_SECRET_ACCESS_KEY",
    )
    parser.add_argument(
        "--aws-session-token-env",
        default="CALLCENTER_AWS_SESSION_TOKEN",
    )
    argv = sys.argv[1:]
    # CDE may pass all job arguments as a single quoted string.
    if len(argv) == 1:
        argv = shlex.split(argv[0])
    return parser.parse_args(argv)


def validate_date(value: Optional[str]) -> str:
    candidate = (value or "").strip()

    # If the CDE/Airflow variable was not substituted, try the environment variable.
    if not candidate or (candidate.startswith("{{") and candidate.endswith("}}")):
        candidate = os.environ.get("INTERACTION_DATE", "").strip()

    if not candidate:
        raise ValueError(
            "No interaction date was supplied. Pass YYYY-MM-DD as the first "
            "job argument or set INTERACTION_DATE."
        )

    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError as exc:
        raise ValueError(
            f"Invalid interaction_date {candidate!r}; expected YYYY-MM-DD"
        ) from exc


# Spark needs s3a://, while Glue stores locations as s3://.
def spark_uri(uri: str) -> str:
    return "s3a://" + uri[5:] if uri.startswith("s3://") else uri


def glue_uri(uri: str) -> str:
    return "s3://" + uri[6:] if uri.startswith("s3a://") else uri


def sibling_uri(uri: str, name: str) -> str:
    return f"{uri.rstrip('/').rsplit('/', 1)[0]}/{name}"


# Keep summary data out of the Hive managed warehouse path.
def external_uri(uri: str) -> str:
    return uri.replace("/tablespace/managed/", "/tablespace/external/")


def ensure_directory(spark: SparkSession, uri: str) -> None:
    # Use the Hadoop FileSystem API so bucket-specific S3A settings apply.
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if fs.exists(path):
        print(f"Output directory exists: {uri}")
        return
    if not fs.mkdirs(path):
        raise RuntimeError(f"Failed to create output directory: {uri}")
    print(f"Created output directory: {uri}")


def spark_conf_value(spark: SparkSession, name: str) -> Optional[str]:
    conf = spark.sparkContext.getConf()
    # Credentials may be supplied as Spark env confs instead of process env vars.
    for key in (f"spark.driverEnv.{name}", f"spark.executorEnv.{name}", name):
        value = conf.get(key, None)
        if value:
            return value
    return None


def resolve_credentials(
    spark: SparkSession, args: argparse.Namespace
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    def resolve(name: str) -> Optional[str]:
        return os.environ.get(name) or spark_conf_value(spark, name)

    return (
        resolve(args.aws_access_key_env),
        resolve(args.aws_secret_key_env),
        resolve(args.aws_session_token_env),
    )


def configure_s3a(
    spark: SparkSession,
    uris: list[str],
    args: argparse.Namespace,
) -> None:
    buckets = {
        urlparse(uri).netloc
        for uri in uris
        if urlparse(uri).scheme in {"s3", "s3a"}
    }
    access_key, secret_key, session_token = resolve_credentials(spark, args)
    if not access_key or not secret_key:
        print("Using the CDE runtime's existing AWS credentials.")

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    # HTTPS Ozone needs a truststore only when this job actually accesses the configured Ozone bucket.
    uses_ozone_bucket = args.ozone_bucket in buckets
    if uses_ozone_bucket and args.ozone_endpoint and args.ozone_endpoint.lower().startswith("https://"):
        truststore = Path(args.ozone_truststore)
        if not truststore.is_file():
            raise FileNotFoundError(f"Ozone truststore not found: {truststore}")
        jvm = spark.sparkContext._jvm
        active_truststore = jvm.java.lang.System.getProperty("javax.net.ssl.trustStore")
        executor_options = spark.sparkContext.getConf().get(
            "spark.executor.extraJavaOptions", ""
        )
        spark_files = spark.sparkContext.getConf().get("spark.files", "")
        executor_truststore = truststore.name
        shared_image_truststore = str(truststore) in executor_options
        localized_truststore = (
            executor_truststore in executor_options
            and executor_truststore in spark_files
        )
        if active_truststore != str(truststore):
            print(
                "WARNING: driver JVM reports javax.net.ssl.trustStore="
                f"{active_truststore or '<unset>'}; expected {truststore}"
            )
        else:
            print(f"Using preconfigured Ozone JVM truststore: {active_truststore}")
        if not (shared_image_truststore or localized_truststore):
            print(
                "WARNING: executor truststore configuration could not be verified; "
                "continuing so the S3A request can test the effective JVM settings"
            )
    provider = (
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
        if session_token
        else "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    # Per-bucket settings let Ozone and AWS S3 buckets coexist in one job.
    for bucket in buckets:
        prefix = f"fs.s3a.bucket.{bucket}."
        if bucket == args.ozone_bucket:
            if args.ozone_endpoint:
                hadoop_conf.set(prefix + "endpoint", args.ozone_endpoint)
                hadoop_conf.set(prefix + "endpoint.region", args.ozone_region)
                hadoop_conf.set(prefix + "path.style.access", "true")
                hadoop_conf.set(
                    prefix + "connection.ssl.enabled",
                    str(args.ozone_endpoint.lower().startswith("https://")).lower(),
                )
            endpoint = (
                hadoop_conf.get(prefix + "endpoint")
                or hadoop_conf.get("fs.s3a.endpoint")
            )
            print(
                f"Using CDE Ozone S3A configuration for {bucket}; "
                f"endpoint: {endpoint or '<injected at runtime>'}"
            )
            continue
        # Non-Ozone buckets are real AWS S3.
        if access_key and secret_key:
            hadoop_conf.set(prefix + "access.key", access_key)
            hadoop_conf.set(prefix + "secret.key", secret_key)
            hadoop_conf.set(prefix + "aws.credentials.provider", provider)
        hadoop_conf.set(prefix + "endpoint", f"s3.{args.aws_region}.amazonaws.com")
        if access_key and secret_key and session_token:
            hadoop_conf.set(prefix + "session.token", session_token)
    aws_buckets = sorted(bucket for bucket in buckets if bucket != args.ozone_bucket)
    if aws_buckets:
        print(f"Configured explicit AWS S3A credentials for: {', '.join(aws_buckets)}")


def glue_call_args(catalog_id: Optional[str]) -> dict[str, str]:
    return {"CatalogId": catalog_id} if catalog_id else {}


def create_glue_client(spark: SparkSession, args: argparse.Namespace):
    """Create a Glue client using credentials configured for this S3 bucket.

    Spark's ``spark.hadoop.fs.s3a.bucket.<bucket>.*`` settings are Hadoop
    S3A settings; boto3 does not automatically consume them. Read the
    corresponding Hadoop values and pass them explicitly to boto3. If those
    values are absent, fall back to CALLCENTER_* settings, then boto3's
    standard credential chain (for example, a CDE-provided IAM role).
    """
    bucket = "ccf-psecoe-ccf-artifacts"
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    prefix = f"fs.s3a.bucket.{bucket}."

    s3a_access_key = hadoop_conf.get(prefix + "access.key")
    s3a_secret_key = hadoop_conf.get(prefix + "secret.key")
    s3a_session_token = hadoop_conf.get(prefix + "session.token")

    env_access_key, env_secret_key, env_session_token = resolve_credentials(spark, args)

    # Prefer the bucket-specific credentials explicitly set in the CDE Spark
    # configuration, because these are the credentials the user configured.
    if s3a_access_key and s3a_secret_key:
        access_key, secret_key, session_token = (
            s3a_access_key, s3a_secret_key, s3a_session_token
        )
        credential_source = f"Spark S3A configuration for bucket {bucket}"
    elif env_access_key and env_secret_key:
        access_key, secret_key, session_token = (
            env_access_key, env_secret_key, env_session_token
        )
        credential_source = "CALLCENTER_* environment/Spark settings"
    else:
        access_key = secret_key = session_token = None
        credential_source = "boto3 default credential chain (CDE runtime credentials)"

    client_args = {"region_name": args.aws_region}
    if access_key and secret_key:
        client_args["aws_access_key_id"] = access_key
        client_args["aws_secret_access_key"] = secret_key
        if session_token:
            client_args["aws_session_token"] = session_token
        print(f"Creating AWS Glue client with credentials from: {credential_source}")
    else:
        print(f"Creating AWS Glue client with: {credential_source}")

    return boto3.client("glue", **client_args)


def get_table_location(glue_client, args: argparse.Namespace) -> str:
    response = glue_client.get_table(
        DatabaseName=args.glue_database,
        Name=args.source_table,
        **glue_call_args(args.glue_catalog_id),
    )
    location = response["Table"].get("StorageDescriptor", {}).get("Location", "")
    if not location:
        raise ValueError(
            f"Glue table {args.glue_database}.{args.source_table} has no S3 location."
        )
    return location


def register_summary_table(
    glue_client,
    args: argparse.Namespace,
    output_uri: str,
) -> None:
    # External table: dropping it in Glue leaves the Parquet data in place.
    table_input = {
        "Name": args.summary_table,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"EXTERNAL": "TRUE", "classification": "parquet"},
        "PartitionKeys": [{"Name": "interactiondate", "Type": "date"}],
        "StorageDescriptor": {
            "Columns": [
                {"Name": "avg_satisfaction_rating", "Type": "double"},
                {"Name": "total_resolved_calls", "Type": "bigint"},
                {"Name": "total_unresolved_calls", "Type": "bigint"},
                {"Name": "avg_duration", "Type": "double"},
                {"Name": "call_count", "Type": "bigint"},
            ],
            "Location": glue_uri(output_uri),
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
        },
    }
    common_args = {
        "DatabaseName": args.glue_database,
        **glue_call_args(args.glue_catalog_id),
    }
    # Upsert: create if missing, otherwise update (also converts an existing managed table).
    try:
        glue_client.get_table(Name=args.summary_table, **common_args)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_table(TableInput=table_input, **common_args)
        print(f"Created Glue table {args.glue_database}.{args.summary_table}")
    else:
        glue_client.update_table(
            TableInput=table_input,
            SkipArchive=True,
            **common_args,
        )
        print(f"Updated Glue table {args.glue_database}.{args.summary_table}")


def register_partition(
    glue_client,
    args: argparse.Namespace,
    partition_uri: str,
    interaction_date: str,
) -> None:
    storage_descriptor = {
        "Columns": [
            {"Name": "avg_satisfaction_rating", "Type": "double"},
            {"Name": "total_resolved_calls", "Type": "bigint"},
            {"Name": "total_unresolved_calls", "Type": "bigint"},
            {"Name": "avg_duration", "Type": "double"},
            {"Name": "call_count", "Type": "bigint"},
        ],
        "Location": glue_uri(partition_uri),
        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "Parameters": {"serialization.format": "1"},
        },
    }
    common_args = {
        "DatabaseName": args.glue_database,
        "TableName": args.summary_table,
        "PartitionValues": [interaction_date],
        **glue_call_args(args.glue_catalog_id),
    }
    # Upsert the partition so reruns for the same date are idempotent.
    try:
        glue_client.get_partition(**common_args)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_partition(
            PartitionInput={
                "Values": [interaction_date],
                "StorageDescriptor": storage_descriptor,
            },
            DatabaseName=args.glue_database,
            TableName=args.summary_table,
            **glue_call_args(args.glue_catalog_id),
        )
    else:
        glue_client.update_partition(
            PartitionValueList=[interaction_date],
            PartitionInput={
                "Values": [interaction_date],
                "StorageDescriptor": storage_descriptor,
            },
            DatabaseName=args.glue_database,
            TableName=args.summary_table,
            **glue_call_args(args.glue_catalog_id),
        )


def run(args: argparse.Namespace) -> None:
    interaction_date = validate_date(args.interaction_date)
    # Keep catalog namespaces independent by default: direct S3 runs use a new database,
    # while the existing Glue/Ozone workflow keeps its original database.
    if not args.glue_database:
        args.glue_database = "callcenter_s3_data" if args.source_s3_uri else "callcenter_data_s3"

    spark = SparkSession.builder.appName("CDE-CallCenter-Summary").getOrCreate()
    glue_client = create_glue_client(spark, args)

    # Keep the existing Ozone path intact unless an explicit S3 source is supplied.
    # The S3 path can be used directly without changing the existing Ozone Glue table.
    if args.source_s3_uri:
        source_uri = args.source_s3_uri
        if not source_uri.startswith(("s3://", "s3a://")):
            raise ValueError("--source-s3-uri must start with s3:// or s3a://")
        print(f"Using direct S3 source location: {source_uri}")
    else:
        source_uri = get_table_location(glue_client, args)
        print(f"Using Glue source table {args.glue_database}.{args.source_table}: {source_uri}")

    output_uri = args.output_s3_uri or external_uri(
        sibling_uri(source_uri, args.summary_table)
    )
    source_uri = spark_uri(source_uri)
    output_uri = spark_uri(output_uri)
    partition_uri = f"{output_uri.rstrip('/')}/interactiondate={interaction_date}"
    configure_s3a(spark, [source_uri, output_uri], args)
    ensure_directory(spark, output_uri)

    print(f"Reading source data from {source_uri}")
    source_df = spark.read.parquet(source_uri)
    # Single-row daily aggregate; issueresolved is a yes/no string.
    summary_df = (
        source_df
        .where(F.to_date(F.col("interactiondate")) == F.lit(interaction_date).cast("date"))
        .agg(
            F.avg("satisfactionrating").alias("avg_satisfaction_rating"),
            F.sum(F.when(F.lower(F.col("issueresolved")) == "yes", 1).otherwise(0))
            .cast("long")
            .alias("total_resolved_calls"),
            F.sum(F.when(F.lower(F.col("issueresolved")) == "no", 1).otherwise(0))
            .cast("long")
            .alias("total_unresolved_calls"),
            F.avg("duration").alias("avg_duration"),
            F.count(F.lit(1)).alias("call_count"),
        )
    )

    # Skip writing an empty partition when the date has no data.
    if summary_df.first()["call_count"] == 0:
        print(f"No call-center records found for {interaction_date}; nothing was written.")
        return

    print(f"Writing summary partition to {partition_uri}")
    # Write directly to the partition path; Glue metadata is registered afterward.
    summary_df.write.mode("overwrite").parquet(partition_uri)
    register_summary_table(glue_client, args, output_uri)
    register_partition(glue_client, args, partition_uri, interaction_date)

    print(f"Results for {interaction_date}:")
    summary_df.show(truncate=False)


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()