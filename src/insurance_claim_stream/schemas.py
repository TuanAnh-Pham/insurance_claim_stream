"""Spark schemas shared by ingestion and processing notebooks."""

from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType, TimestampType

CLAIM_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("claim_id", StringType(), False),
        StructField("policy_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("sequence_number", LongType(), False),
        StructField("claim_amount", DecimalType(18, 2), True),
        StructField("status", StringType(), False),
        StructField("source", StringType(), True),
    ]
)

ALLOWED_EVENT_TYPES = ("CLAIM_OPENED", "CLAIM_UPDATED", "CLAIM_APPROVED", "CLAIM_DENIED", "CLAIM_CLOSED")
ALLOWED_STATUSES = ("OPEN", "UNDER_REVIEW", "APPROVED", "DENIED", "CLOSED")
