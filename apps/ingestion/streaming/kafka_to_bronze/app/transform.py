from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.bronze_mapper import map_to_bronze
from common.constants import EVENT_TYPE_TRIP_COMPLETED, EVENT_TYPE_TRIP_STARTED


EVENT_TYPE_BY_KIND = {
    "started": EVENT_TYPE_TRIP_STARTED,
    "completed": EVENT_TYPE_TRIP_COMPLETED,
}


def transform(df: DataFrame, event_kind: str) -> DataFrame:
    expected_event_type = EVENT_TYPE_BY_KIND[event_kind]
    return (
        map_to_bronze(
            df.filter(F.col("event_type") == F.lit(expected_event_type)),
            event_kind,
        )
    )
