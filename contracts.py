"""
contracts.py - Data Contracts & Schema Validation for MP 5 Structured Streaming.

This module defines the explicit PySpark StructType schema for CorpusChunk text records
and provides deal contract validation functions for BoundedChunking and provenance whitelist gates.
"""

from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Default whitelist of approved data sources for the Predictive Text Corpora track
APPROVED_SOURCE_WHITELIST: List[str] = [
    "wikimedia/wikipedia",
    "huggingface/ag_news",
    "huggingface/cnn_dailymail",
    "open_meteo_archive",
    "usgs_earthquake_feed",
    "wanderbricks_snapshot",
]

# Standard maximum chunk duration in seconds per BoundedChunking deal contract
MAX_CHUNK_DURATION_SEC: int = 30


def get_corpus_chunk_schema() -> StructType:
    """
    Returns the explicit StructType schema for CorpusChunk text records.
    
    Fields & Nullability Justification:
    - record_id (StringType, nullable=False): Primary key identifier; required for deduplication & joining.
    - track (StringType, nullable=False): Architectural track identifier (must be 'Predictive' or 'VDB').
    - source_id (StringType, nullable=False): Origin dataset slug or endpoint name; validated against whitelist.
    - ts_start (TimestampType, nullable=False): Window start event-time; required for windowing.
    - ts_end (TimestampType, nullable=False): Window end event-time; required for watermarking.
    - text (StringType, nullable=False): Cleaned chunk text payload; core input for embeddings/classification.
    - ingest_ts (TimestampType, nullable=False): System ingestion timestamp; required for lateness measurement.
    - license_class (StringType, nullable=True): Optional licensing metadata (e.g., 'CC-BY-SA', 'MIT').
    """
    return StructType(
        [
            StructField("record_id", StringType(), nullable=False),
            StructField("track", StringType(), nullable=False),
            StructField("source_id", StringType(), nullable=False),
            StructField("ts_start", TimestampType(), nullable=False),
            StructField("ts_end", TimestampType(), nullable=False),
            StructField("text", StringType(), nullable=False),
            StructField("ingest_ts", TimestampType(), nullable=False),
            StructField("license_class", StringType(), nullable=True),
        ]
    )


def validate_bounded_chunking(df: DataFrame, max_duration_sec: int = MAX_CHUNK_DURATION_SEC) -> DataFrame:
    """
    Applies the BoundedChunking deal contract to a PySpark DataFrame.
    
    Contract Rules:
    1. ts_start must be strictly earlier than ts_end (ts_start < ts_end).
    2. Chunk duration (ts_end - ts_start) must be <= max_duration_sec (30 seconds).
    
    Returns a DataFrame with a boolean column 'is_bounded_chunk_valid'.
    """
    duration_expr = F.col("ts_end").cast("long") - F.col("ts_start").cast("long")
    
    valid_bounds_condition = (
        (F.col("ts_start") < F.col("ts_end")) &
        (duration_expr > 0) &
        (duration_expr <= max_duration_sec)
    )
    
    return df.withColumn("is_bounded_chunk_valid", valid_bounds_condition)


def validate_provenance_and_whitelist(
    df: DataFrame,
    allowed_sources: Optional[List[str]] = None,
    expected_track: str = "Predictive"
) -> DataFrame:
    """
    Applies provenance and whitelist gate deal contracts to a PySpark DataFrame.
    
    Contract Rules:
    1. source_id must exist within the allowed_sources whitelist.
    2. track must match the expected track (default: 'Predictive').
    
    Returns a DataFrame with a boolean column 'is_provenance_valid'.
    """
    whitelist = allowed_sources if allowed_sources is not None else APPROVED_SOURCE_WHITELIST
    
    provenance_condition = (
        F.col("source_id").isin(whitelist) &
        (F.col("track") == expected_track)
    )
    
    return df.withColumn("is_provenance_valid", provenance_condition)


def filter_valid_records(df: DataFrame, allowed_sources: Optional[List[str]] = None) -> DataFrame:
    """
    Convenience method that evaluates both BoundedChunking and Provenance contracts
    and filters out non-compliant records.
    
    Returns a DataFrame containing only records that pass all deal contracts.
    """
    df_bounded = validate_bounded_chunking(df)
    df_validated = validate_provenance_and_whitelist(df_bounded, allowed_sources=allowed_sources)
    
    # Filter for records passing both deal contracts and drop temporary validation flags
    valid_df = df_validated.filter(
        (F.col("is_bounded_chunk_valid") == True) &
        (F.col("is_provenance_valid") == True)
    ).drop("is_bounded_chunk_valid", "is_provenance_valid")
    
    return valid_df


if __name__ == "__main__":
    print("CorpusChunk StructType Schema:")
    schema = get_corpus_chunk_schema()
    for field in schema.fields:
        print(f" - {field.name}: {field.dataType.simpleString()} (nullable={field.nullable})")