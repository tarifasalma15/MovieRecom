# Description: This script is the main pipeline for the ETL process. It loads the datasets, transforms them and saves them to CSV files.
# Instructions: Run the script to execute the ETL process.
# Output: CSV files containing the transformed datasets.
# Reference: This script is a part of the ETL pipeline in the Movie Recommendation System project.
# Disclaimer: The script is for demonstration purposes and may need to be modified according to specific use cases (if it is a real-time collection, the data source may change).


from ml_ingestion import load_datasets
from ml_transformation import transform_merge
from ml_storage import save_to_csv
from pyspark.sql import SparkSession

def main():
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .config("spark.driver.host", "localhost") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .appName('Movie Pipeline') \
        .master("local[*]") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.shuffle.partitions", "500")  # Default is 200

    # Ingestion
    cast, crew, keywords, links, links_small, movies_metadata, ratings, ratings_small = load_datasets(spark)

    # Transformation
    data = transform_merge(cast, crew, keywords, links, links_small, movies_metadata, ratings, ratings_small)

    # data.show()
    data.printSchema()

    # Storage
    save_to_csv(data, "ml_pipeline/ml_dataset")
    
    spark.stop()

if __name__ == "__main__":
    main()
