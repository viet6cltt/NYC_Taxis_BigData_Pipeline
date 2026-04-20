from app.spark_session import build_spark_session
from app.reader import read_from_bronze
from app.writer import write_to_silver
from app.transform import transform

def main() -> None:
    spark = build_spark_session()
    
    bronze_df = read_from_bronze(spark)
    
    silver_df = transform(bronze_df)
    
    write_to_silver(silver_df)
    
if __name__ == "__main__":
    main()
