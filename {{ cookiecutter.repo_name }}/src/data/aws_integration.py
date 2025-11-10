"""
AWS Integration Module for MLOps Pipeline

This module provides utilities for interacting with AWS services:
- S3: Data storage and retrieval
- Athena: SQL queries on data lake
- Iceberg: Table format for analytics
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from loguru import logger

try:
    import awswrangler as wr
except ImportError:
    logger.warning("awswrangler not installed. Some features may not be available.")
    wr = None


class S3Client:
    """Client for interacting with AWS S3."""

    def __init__(
        self,
        bucket: Optional[str] = None,
        region: Optional[str] = None,
        profile: Optional[str] = None,
    ):
        """
        Initialize S3 client.

        Args:
            bucket: Default S3 bucket name
            region: AWS region
            profile: AWS profile name
        """
        self.bucket = bucket or os.getenv("S3_ML_ARTIFACTS_BUCKET")
        self.region = region or os.getenv("AWS_REGION", "us-east-1")

        session = boto3.Session(
            profile_name=profile or os.getenv("AWS_PROFILE"),
            region_name=self.region,
        )
        self.s3_client = session.client("s3")
        self.s3_resource = session.resource("s3")

    def upload_file(
        self,
        local_path: Union[str, Path],
        s3_key: str,
        bucket: Optional[str] = None,
        extra_args: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Upload a file to S3.

        Args:
            local_path: Path to local file
            s3_key: S3 object key
            bucket: S3 bucket (uses default if not specified)
            extra_args: Extra arguments for upload

        Returns:
            S3 URI of uploaded file
        """
        bucket = bucket or self.bucket
        try:
            self.s3_client.upload_file(
                str(local_path),
                bucket,
                s3_key,
                ExtraArgs=extra_args or {},
            )
            s3_uri = f"s3://{bucket}/{s3_key}"
            logger.info(f"Uploaded {local_path} to {s3_uri}")
            return s3_uri
        except ClientError as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            raise

    def download_file(
        self,
        s3_key: str,
        local_path: Union[str, Path],
        bucket: Optional[str] = None,
    ) -> Path:
        """
        Download a file from S3.

        Args:
            s3_key: S3 object key
            local_path: Local destination path
            bucket: S3 bucket (uses default if not specified)

        Returns:
            Path to downloaded file
        """
        bucket = bucket or self.bucket
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            self.s3_client.download_file(bucket, s3_key, str(local_path))
            logger.info(f"Downloaded s3://{bucket}/{s3_key} to {local_path}")
            return local_path
        except ClientError as e:
            logger.error(f"Failed to download {s3_key}: {e}")
            raise

    def list_objects(
        self,
        prefix: str = "",
        bucket: Optional[str] = None,
        suffix: Optional[str] = None,
    ) -> List[str]:
        """
        List objects in S3 bucket.

        Args:
            prefix: S3 key prefix
            bucket: S3 bucket (uses default if not specified)
            suffix: Filter by suffix (e.g., '.parquet')

        Returns:
            List of S3 keys
        """
        bucket = bucket or self.bucket
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            objects = [obj["Key"] for obj in response.get("Contents", [])]

            if suffix:
                objects = [obj for obj in objects if obj.endswith(suffix)]

            return objects
        except ClientError as e:
            logger.error(f"Failed to list objects with prefix {prefix}: {e}")
            raise

    def read_parquet(
        self,
        s3_key: str,
        bucket: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Read parquet file from S3.

        Args:
            s3_key: S3 object key
            bucket: S3 bucket (uses default if not specified)

        Returns:
            DataFrame
        """
        bucket = bucket or self.bucket
        s3_uri = f"s3://{bucket}/{s3_key}"

        if wr is None:
            raise ImportError("awswrangler is required to read parquet from S3")

        try:
            df = wr.s3.read_parquet(s3_uri)
            logger.info(f"Read parquet from {s3_uri}: {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Failed to read parquet from {s3_uri}: {e}")
            raise

    def write_parquet(
        self,
        df: pd.DataFrame,
        s3_key: str,
        bucket: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        """
        Write DataFrame to S3 as parquet.

        Args:
            df: DataFrame to write
            s3_key: S3 object key
            bucket: S3 bucket (uses default if not specified)
            **kwargs: Additional arguments for write_parquet

        Returns:
            S3 URI of written file
        """
        bucket = bucket or self.bucket
        s3_uri = f"s3://{bucket}/{s3_key}"

        if wr is None:
            raise ImportError("awswrangler is required to write parquet to S3")

        try:
            wr.s3.to_parquet(df, s3_uri, **kwargs)
            logger.info(f"Wrote {len(df)} rows to {s3_uri}")
            return s3_uri
        except Exception as e:
            logger.error(f"Failed to write parquet to {s3_uri}: {e}")
            raise


class AthenaClient:
    """Client for interacting with AWS Athena."""

    def __init__(
        self,
        database: Optional[str] = None,
        workgroup: Optional[str] = None,
        output_location: Optional[str] = None,
        region: Optional[str] = None,
        profile: Optional[str] = None,
    ):
        """
        Initialize Athena client.

        Args:
            database: Athena database name
            workgroup: Athena workgroup
            output_location: S3 location for query results
            region: AWS region
            profile: AWS profile name
        """
        self.database = database or os.getenv("ATHENA_DATABASE", "ml_database")
        self.workgroup = workgroup or os.getenv("ATHENA_WORKGROUP", "primary")
        self.output_location = output_location or os.getenv("ATHENA_OUTPUT_LOCATION")
        self.region = region or os.getenv("AWS_REGION", "us-east-1")

        if wr is None:
            logger.warning("awswrangler not installed. Athena features limited.")

        session = boto3.Session(
            profile_name=profile or os.getenv("AWS_PROFILE"),
            region_name=self.region,
        )
        self.athena_client = session.client("athena")

    def query(
        self,
        sql: str,
        database: Optional[str] = None,
        workgroup: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Execute SQL query on Athena.

        Args:
            sql: SQL query string
            database: Athena database (uses default if not specified)
            workgroup: Athena workgroup (uses default if not specified)

        Returns:
            Query results as DataFrame
        """
        if wr is None:
            raise ImportError("awswrangler is required for Athena queries")

        database = database or self.database
        workgroup = workgroup or self.workgroup

        try:
            df = wr.athena.read_sql_query(
                sql=sql,
                database=database,
                workgroup=workgroup,
            )
            logger.info(f"Query executed successfully: {len(df)} rows returned")
            return df
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise

    def create_iceberg_table(
        self,
        table_name: str,
        schema: Dict[str, str],
        location: str,
        partitions: Optional[List[str]] = None,
    ) -> None:
        """
        Create Iceberg table in Athena.

        Args:
            table_name: Name of the table
            schema: Dictionary of column names and types
            location: S3 location for table data
            partitions: List of partition columns
        """
        columns = ", ".join([f"{col} {dtype}" for col, dtype in schema.items()])

        partition_clause = ""
        if partitions:
            partition_clause = f"PARTITIONED BY ({', '.join(partitions)})"

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}
        )
        {partition_clause}
        LOCATION '{location}'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'parquet',
            'write_compression' = 'snappy'
        )
        """

        try:
            self.query(ddl)
            logger.info(f"Created Iceberg table: {table_name}")
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise


class IcebergManager:
    """Manager for Iceberg tables on AWS."""

    def __init__(
        self,
        s3_client: Optional[S3Client] = None,
        athena_client: Optional[AthenaClient] = None,
    ):
        """
        Initialize Iceberg manager.

        Args:
            s3_client: S3 client instance
            athena_client: Athena client instance
        """
        self.s3 = s3_client or S3Client()
        self.athena = athena_client or AthenaClient()

    def write_features(
        self,
        df: pd.DataFrame,
        table_name: str,
        mode: str = "append",
    ) -> None:
        """
        Write features to Iceberg table.

        Args:
            df: DataFrame with features
            table_name: Name of Iceberg table
            mode: Write mode ('append' or 'overwrite')
        """
        if wr is None:
            raise ImportError("awswrangler is required for Iceberg operations")

        try:
            wr.athena.to_iceberg(
                df=df,
                database=self.athena.database,
                table=table_name,
                temp_path=f"s3://{self.s3.bucket}/temp/",
                mode=mode,
            )
            logger.info(f"Wrote {len(df)} rows to Iceberg table {table_name}")
        except Exception as e:
            logger.error(f"Failed to write to Iceberg table {table_name}: {e}")
            raise

    def read_features(
        self,
        table_name: str,
        filters: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Read features from Iceberg table.

        Args:
            table_name: Name of Iceberg table
            filters: SQL WHERE clause for filtering

        Returns:
            DataFrame with features
        """
        sql = f"SELECT * FROM {table_name}"
        if filters:
            sql += f" WHERE {filters}"

        return self.athena.query(sql)


# Convenience functions
def get_s3_client(**kwargs: Any) -> S3Client:
    """Get configured S3 client."""
    return S3Client(**kwargs)


def get_athena_client(**kwargs: Any) -> AthenaClient:
    """Get configured Athena client."""
    return AthenaClient(**kwargs)


def get_iceberg_manager(**kwargs: Any) -> IcebergManager:
    """Get configured Iceberg manager."""
    return IcebergManager(**kwargs)
