"""Pure DataFrame transformations used by the medallion pipeline."""

from pyspark.sql import DataFrame, Window, functions as F


def deduplicate_events(df: DataFrame) -> DataFrame:
    """Keep the latest ingested copy of each immutable event identifier."""
    window = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc(), F.col("source_file").desc())
    return df.withColumn("_event_rank", F.row_number().over(window)).where("_event_rank = 1").drop("_event_rank")


def latest_claim_events(df: DataFrame) -> DataFrame:
    """Select the deterministic latest event for every claim."""
    window = Window.partitionBy("claim_id").orderBy(
        F.col("sequence_number").desc(), F.col("event_time").desc(), F.col("event_id").desc()
    )
    return df.withColumn("_claim_rank", F.row_number().over(window)).where("_claim_rank = 1").drop("_claim_rank")
