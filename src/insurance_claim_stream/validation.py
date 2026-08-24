"""Data-quality expressions for claim events."""

from pyspark.sql import Column, DataFrame, functions as F

from insurance_claim_stream.schemas import ALLOWED_EVENT_TYPES, ALLOWED_STATUSES


def validation_error() -> Column:
    """Return a nullable explanation for the first failed business rule."""
    return (
        F.when(F.col("event_id").isNull(), F.lit("missing event_id"))
        .when(F.col("claim_id").isNull(), F.lit("missing claim_id"))
        .when(~F.col("event_type").isin(*ALLOWED_EVENT_TYPES), F.lit("invalid event_type"))
        .when(~F.col("status").isin(*ALLOWED_STATUSES), F.lit("invalid status"))
        .when(F.col("sequence_number") < 1, F.lit("invalid sequence_number"))
        .when(F.col("claim_amount") < 0, F.lit("negative claim_amount"))
    )


def with_validation(df: DataFrame) -> DataFrame:
    """Annotate records without discarding evidence needed for quarantine."""
    return df.withColumn("validation_error", validation_error()).withColumn(
        "is_valid", F.col("validation_error").isNull()
    )
