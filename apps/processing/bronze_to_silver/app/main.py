from app.spark_session import build_spark_session
from app.reader import read_bronze_batch, read_bronze_streaming
from app.writer import write_to_silver_batch, write_to_silver_streaming
from app.transform import transform
from app.config import PIPELINE_MODE
def main() -> None:
    spark = build_spark_session(PIPELINE_MODE)
    
    if PIPELINE_MODE == "batch":
        bronze_df = read_bronze_batch(spark)
    else:
        bronze_df = read_bronze_streaming(spark)
    
    silver_df = transform(bronze_df, PIPELINE_MODE)
    
    if PIPELINE_MODE == "batch":
        write_to_silver_batch(silver_df)
    else:
        write_to_silver_streaming(silver_df)
    
if __name__ == "__main__":
    main()
