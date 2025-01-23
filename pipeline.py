# Description: This script is the main pipeline for the ETL process. It loads the datasets, transforms them and saves them to CSV files.
# Instructions: Run the script to execute the ETL process.
# Output: CSV files containing the transformed datasets.
# Reference: This script is a part of the ETL pipeline in the Movie Recommendation System project.
# Disclaimer: The script is for demonstration purposes and may need to be modified according to specific use cases (if it is a real-time collection, the data source may change).


from ingestion import load_datasets
from transformation \
    import transform_credits, transform_keywords,\
        transform_links, transform_links_small,\
        transform_movies_metadata, transform_ratings,\
        transform_ratings_small
from storage import save_to_csv
from pyspark.sql import SparkSession

def main():
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .config("spark.driver.host", "localhost") \
        .appName('Movie Pipeline') \
        .master("local[*]") \
        .getOrCreate()

    # Ingestion
    credits, keywords, links, links_small, movies_metadata, ratings, ratings_small = load_datasets(spark)

    # Transformation
    cast, crew = transform_credits(credits)
    keywords = transform_keywords(keywords)
    links = transform_links(links)
    links_small = transform_links_small(links_small)
    movies_metadata = transform_movies_metadata(movies_metadata)
    ratings = transform_ratings(ratings)
    ratings_small = transform_ratings_small(ratings_small)

    # Storage
    save_to_csv(cast, "datasets/cast")
    save_to_csv(crew, "datasets/crew")
    save_to_csv(keywords, "datasets/keywords")
    save_to_csv(links, "datasets/links")
    save_to_csv(links_small, "datasets/links_small")
    save_to_csv(movies_metadata, "datasets/movies_metadata")
    save_to_csv(ratings, "datasets/ratings")
    save_to_csv(ratings_small, "datasets/ratings_small")

    spark.stop()

if __name__ == "__main__":
    main()
