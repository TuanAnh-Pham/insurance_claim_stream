"""Delta Lake merge helpers for materialized claim state."""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def merge_latest_claim_state(spark: SparkSession, updates: DataFrame, table_name: str) -> None:
    """Apply only events newer than the state already stored for a claim."""
    target = DeltaTable.forName(spark, table_name)
    newer = "s.sequence_number > t.sequence_number OR (s.sequence_number = t.sequence_number AND s.event_time > t.event_time)"
    (
        target.alias("t")
        .merge(updates.alias("s"), "t.claim_id = s.claim_id")
        .whenMatchedUpdateAll(condition=newer)
        .whenNotMatchedInsertAll()
        .execute()
    )
