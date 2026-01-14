"""
MinIO Reader for Hive-Partitioned Raw Events.

Reads Parquet files from MinIO with automatic partition discovery,
date range filtering, and OpenTelemetry tracing.
"""

from datetime import date
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.batch.observability import get_tracer
from src.utils.logger import get_logger

logger = get_logger(__name__)
tracer = get_tracer()


class MinIOReader:
    """
    Reads Hive-partitioned raw events from MinIO.

    Expected partition structure:
        s3a://raw-events/event_type=X/year=Y/month=M/day=D/hour=H/*.parquet

    Usage:
        reader = MinIOReader(spark)
        df = reader.read_events(
            start_date=date(2024, 12, 1),
            end_date=date(2024, 12, 15),
            event_types=["experience", "engagement"]
        )
    """

    def __init__(
        self,
        spark: SparkSession,
        bucket: str = "raw-events",
        base_path: str | None = None,
    ):
        """
        Initialize MinIO reader.

        Args:
            spark: Active SparkSession
            bucket: MinIO bucket name
            base_path: Override full base path (e.g., s3a://raw-events)
        """
        self.spark = spark
        self.bucket = bucket
        self.base_path = base_path or f"s3a://{bucket}"
        self.logger = get_logger(self.__class__.__name__)

    def read_events(
        self,
        start_date: date,
        end_date: date,
        event_types: list[str] | None = None,
    ) -> DataFrame:
        """
        Read raw events from MinIO with partition pruning.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            event_types: Optional list of event types to filter

        Returns:
            DataFrame with raw events
        """
        self.logger.info(
            "Reading events from MinIO",
            base_path=self.base_path,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            event_types=event_types,
        )

        # Get list of event type partitions to read
        partitions_to_read = self._list_event_type_partitions(event_types)

        if not partitions_to_read:
            self.logger.warning("No event type partitions found in MinIO")
            # Return empty DataFrame with expected schema
            return self.spark.createDataFrame([], schema="event_type STRING")

        self.logger.info(f"Found {len(partitions_to_read)} event type partitions to read")

        # Read each partition explicitly and union
        dfs = []
        for partition_path in partitions_to_read:
            try:
                # Extract event_type from path: s3a://.../event_type=abc maps to abc
                event_type_val = partition_path.split("event_type=")[-1].strip("/")
                try:
                    df = (
                        self.spark.read.option("mergeSchema", "true")
                        .option(
                            "recursiveFileLookup", "true"
                        )  # Ensure that find files deep in subdirs
                        .parquet(partition_path)
                    )
                    # Trigger schema validation
                    _ = df.schema
                except Exception as e:
                    # Check for schema merge error
                    err_str = str(e).upper()
                    if "MERGE" in err_str or "INCOMPATIBLE" in err_str:
                        self.logger.warning(
                            f"Schema merge failed for {partition_path}, falling back to robust read. Error: {e}"
                        )
                        df = self._read_partition_robust(partition_path, event_type_val)
                    else:
                        raise e

                # Use a different name to avoid overwriting proto's event_type field
                # The proto's event_type contains values like EVENT_TYPE_VIEW, EVENT_TYPE_CLICK
                df = df.withColumn("partition_event_type", F.lit(event_type_val))

                dfs.append(df)
                self.logger.debug(
                    f"Read partition: {partition_path} for event_type={event_type_val}"
                )
            except Exception as e:
                self.logger.warning(f"Failed to read partition {partition_path}: {e}")

        if not dfs:
            self.logger.warning("No data read from any partition")
            return self.spark.createDataFrame([], schema="event_type STRING")

        # Union all DataFrames
        df = dfs[0]
        for other_df in dfs[1:]:
            df = df.unionByName(other_df, allowMissingColumns=True)

        # Ensure event_type column exists (use proto's event_type if available, else fall back to partition)
        # The proto's EventType enum contains values like EVENT_TYPE_VIEW, EVENT_TYPE_CLICK
        if "event_type" not in df.columns:
            self.logger.warning("event_type column missing from data, using partition_event_type")
            df = df.withColumn("event_type", F.col("partition_event_type"))

        # Parse timestamp column first (needed if partition columns are missing)
        df = self._normalize_timestamp(df)

        # Apply date filters
        df = self._apply_date_filter(df, start_date, end_date)

        # Log sample count (non-blocking)
        try:
            sample_count = df.limit(100).count()
            self.logger.info("Events sample read from MinIO", sample_count=sample_count)
        except Exception as e:
            self.logger.warning(f"Could not get sample count: {e}")

        return df

    def _read_partition_robust(self, path: str, event_type_val: str) -> DataFrame:
        """
        Robustly read a partition causing schema merge errors by grouping files with compatible schemas.
        """
        self.logger.warning(f"Starting robust fallback read for {path}")

        # 1. Discover all files
        try:
            file_paths_df = (
                self.spark.read.format("binaryFile")
                .option("pathGlobFilter", "*.parquet")
                .option("recursiveFileLookup", "true")
                .load(path)
                .select("path")
            )
            found_files = [row.path for row in file_paths_df.collect()]
        except Exception as e:
            self.logger.warning(f"Binary file listing failed: {e}. Fallback to path.")
            found_files = [path]

        if not found_files:
            self.logger.warning(f"No parquet files found via binaryFile reader in {path}")
            return self.spark.createDataFrame([], schema="event_type STRING")

        # 2. Group files by "Schema Signature" of complex columns
        # Signature = Tuple of (col_name, is_array_or_struct) for all complex cols present
        complex_cols = [
            "interests",
            "tags",
            "recommendations",
            "current_address_coordinate",
            "event_location_coordinate",
        ]

        # Map: signature -> list of files
        # Signature is a frozen set of (col, type_str) tuples, or similar.
        file_groups: dict[tuple[tuple[str, str], ...], list[str]] = {}

        for p in found_files:
            try:
                # Read schema only (lazy)
                # Use mergeSchema=false to get exact file schema
                schema = self.spark.read.option("mergeSchema", "false").parquet(p).schema

                sig_parts = []
                for col in complex_cols:
                    if col in schema.names:
                        dtype = schema[col].dataType
                        if isinstance(dtype, F.ArrayType | F.StructType):
                            sig_parts.append((col, "complex"))
                        else:
                            sig_parts.append((col, "simple"))  # String or other
                    else:
                        sig_parts.append((col, "missing"))

                signature = tuple(sig_parts)

                if signature not in file_groups:
                    file_groups[signature] = []
                file_groups[signature].append(p)

            except Exception as e:
                self.logger.warning(
                    f"Failed to check schema for {p}: {e}. treating as separate group"
                )
                # Fallback: treat this file as unique group to handle individually
                # Use tuple key to match dict type (unique per file)
                error_sig: tuple[tuple[str, str], ...] = (("error", p),)
                file_groups[error_sig] = [p]

        self.logger.info(
            f"Robust read: grouped {len(found_files)} files into {len(file_groups)} schema groups"
        )

        dfs_to_union = []

        # 3. Process each group
        for sig, files in file_groups.items():
            if not files:
                continue

            try:
                # Bulk read this group
                # mergeSchema=true is safe here because we grouped by conflicting types
                df_group = self.spark.read.option("mergeSchema", "true").parquet(*files)

                # Apply normalization if needed
                # can determine conversion needs from the signature (if it's a tuple)
                # or just inspect the resulting DF schema

                cols_to_convert = []
                for col_name in complex_cols:
                    if col_name in df_group.columns:
                        dtype = df_group.schema[col_name].dataType
                        if isinstance(dtype, F.ArrayType | F.StructType):
                            cols_to_convert.append(col_name)

                if cols_to_convert:
                    # self.logger.info(f"Converting cols {cols_to_convert} to JSON for group {sig}")
                    for c in cols_to_convert:
                        df_group = df_group.withColumn(c, F.to_json(F.col(c)))

                dfs_to_union.append(df_group)

            except Exception as e:
                self.logger.error(
                    f"Bulk read failed for group {sig}: {e}. Falling back to iterative."
                )
                # Fallback to file-by-file for this failed group
                for f in files:
                    try:
                        d = self.spark.read.parquet(f)
                        for col_name in complex_cols:
                            if col_name in d.columns and isinstance(
                                d.schema[col_name].dataType, F.ArrayType | F.StructType
                            ):
                                d = d.withColumn(col_name, F.to_json(F.col(col_name)))
                        dfs_to_union.append(d)
                    except Exception:
                        pass

        if not dfs_to_union:
            self.logger.warning(f"No data could be loaded from {path}")
            return self.spark.createDataFrame([], schema="event_type STRING")

        # 4. Union
        full_df = dfs_to_union[0]
        for other in dfs_to_union[1:]:
            full_df = full_df.unionByName(other, allowMissingColumns=True)

        # Add partition event type if missing
        full_df = full_df.withColumn("partition_event_type", F.lit(event_type_val))
        return full_df

    def _list_event_type_partitions(self, event_types: list[str] | None = None) -> list[str]:
        """
        List event_type partition paths from MinIO using Hadoop API.

        Returns paths like: s3a://raw-events/event_type=like/
        """
        try:
            self.logger.info(f"debug: Starting partition listing for {self.base_path}")

            sc = self.spark.sparkContext
            hadoop_conf = sc._jsc.hadoopConfiguration()

            path = sc._jvm.org.apache.hadoop.fs.Path(self.base_path)  # type: ignore[union-attr]
            fs = path.getFileSystem(hadoop_conf)

            # Set a timeout for the list operation if possible? No, but we can log before.
            status_list = fs.listStatus(path)

            partitions = []
            for status in status_list:
                partition_name = status.getPath().getName()
                # Only include event_type= partitions
                if partition_name.startswith("event_type="):
                    event_type_value = partition_name.replace("event_type=", "")
                    # Filter if specific event types requested
                    if event_types is None or event_type_value in event_types:
                        partitions.append(status.getPath().toString())

            self.logger.info(f"Listed {len(partitions)} event type partitions")
            return partitions

        except Exception as e:
            self.logger.error(f"Failed to list partitions: {e}")
            import traceback

            self.logger.error(traceback.format_exc())
            return []

    def _apply_date_filter(self, df: DataFrame, start_date: date, end_date: date) -> DataFrame:
        """Apply date range filter using partition columns or event_timestamp."""
        # Check if we have partition columns (year, month, day)
        has_partitions = all(col in df.columns for col in ["year", "month", "day"])

        if has_partitions:
            # Create date column from partitions for efficient filtering
            df = df.withColumn(
                "_partition_date",
                F.to_date(
                    F.concat_ws(
                        "-",
                        F.col("year"),
                        F.lpad(F.col("month"), 2, "0"),
                        F.lpad(F.col("day"), 2, "0"),
                    )
                ),
            )
            # Filter by partition date
            df = df.filter(
                (F.col("_partition_date") >= F.lit(start_date))
                & (F.col("_partition_date") <= F.lit(end_date))
            )
        else:
            # Fallback to creating _partition_date from event_timestamp if available
            if "event_timestamp" in df.columns:
                df = df.filter(
                    (F.to_date(F.col("event_timestamp")) >= F.lit(start_date))
                    & (F.to_date(F.col("event_timestamp")) <= F.lit(end_date))
                )
            else:
                self.logger.warning(
                    "No partition columns or event_timestamp found. Skipping date filter."
                )

        return df

    def _normalize_timestamp(self, df: DataFrame) -> DataFrame:
        """Normalize timestamp column to proper timestamp type."""
        if "timestamp" in df.columns:
            # Protobuf timestamp is int64 milliseconds since epoch
            # Convert to proper Spark timestamp
            df = df.withColumn(
                "event_timestamp",
                F.coalesce(
                    # Try as milliseconds (most common from protobuf)
                    F.to_timestamp(F.from_unixtime(F.col("timestamp") / 1000)),
                    # Fallback: try as string/ISO format
                    F.to_timestamp(F.col("timestamp")),
                    # Last resort: current time
                    F.current_timestamp(),
                ),
            )
        elif "_timestamp" in df.columns:
            # Envelope timestamp from Flink parser
            df = df.withColumn(
                "event_timestamp",
                F.to_timestamp(F.from_unixtime(F.col("_timestamp") / 1000)),
            )
        else:
            # Use partition date as fallback
            self.logger.warning("No timestamp column found, using current_timestamp")
            df = df.withColumn(
                "event_timestamp",
                F.current_timestamp(),
            )

        return df

    def read_latest_partition(self, event_type: str) -> DataFrame:
        """
        Read only the latest partition for a given event type.

        Useful for incremental updates.

        Args:
            event_type: Event type to read

        Returns:
            DataFrame with latest partition data
        """
        with tracer.start_as_current_span("read_latest_partition") as span:
            span.set_attribute("event_type", event_type)

            path = f"{self.base_path}/event_type={event_type}"

            # Read and find max date
            df = self.spark.read.parquet(f"{path}/*/*/*")

            # Get max partition date
            max_date = df.select(
                F.max(
                    F.to_date(
                        F.concat_ws(
                            "-",
                            F.col("year"),
                            F.lpad(F.col("month"), 2, "0"),
                            F.lpad(F.col("day"), 2, "0"),
                        )
                    )
                ).alias("max_date")
            ).collect()[0]["max_date"]

            if max_date:
                df = self._apply_date_filter(df, max_date, max_date)
                df = self._normalize_timestamp(df)

            span.set_attribute("max_date", str(max_date))

            return df

    def list_partitions(self, event_type: str | None = None) -> list[dict[str, Any]]:
        """
        List available partitions in the bucket.

        Args:
            event_type: Optional filter by event type

        Returns:
            List of partition metadata
        """
        path = self.base_path
        if event_type:
            path = f"{path}/event_type={event_type}"

        try:
            df = self.spark.read.parquet(f"{path}/*/*/*")
            partitions = df.select("event_type", "year", "month", "day").distinct().collect()
            return [row.asDict() for row in partitions]
        except Exception as e:
            self.logger.warning(f"Failed to list partitions: {e}")
            return []
