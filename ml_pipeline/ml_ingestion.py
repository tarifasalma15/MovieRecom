# 
# This file is used to handle data collection by extracting the datasets from the source.
#
# This file contains 1 function:
# 
# load_datasets(): To load all datasets
# 


from pyspark.sql import SparkSession

def load_datasets(spark):
    # Load datasets using Spark
    cast = spark.read.csv("datasets/cast/part-00000-e10f61b5-f138-4292-b432-f8aa0be57405-c000.csv", header=True, inferSchema=True)
    crew = spark.read.csv("datasets/crew/part-00000-b1de6434-c2d9-4151-b7bf-5b6a65844b9a-c000.csv", header=True, inferSchema=True)
    keywords = spark.read.csv("datasets/keywords/part-00000-07ef1fbc-dfd2-4737-af18-e7739ed0da62-c000.csv", header=True, inferSchema=True)
    links = spark.read.csv("datasets/links/part-00000-aef704cb-7d37-4fe1-a1d4-aef1c6b59280-c000.csv", header=True, inferSchema=True)
    links_small = spark.read.csv("datasets/links_small/part-00000-fc063b55-bc47-4428-9e69-ad7089f8a5e4-c000.csv", header=True, inferSchema=True)
    movies_metadata = spark.read.csv("datasets/movies_metadata/movies_metadata.csv", header=True, inferSchema=True)
    ratings = spark.read.csv("datasets/ratings/part-00000-2636004a-7e43-4f17-8fe6-fd1956f53698-c000.csv", header=True, inferSchema=True)
    ratings_small = spark.read.csv("datasets/ratings_small/part-00000-b96866de-9302-4f7b-9ada-b6fbab0105b5-c000.csv", header=True, inferSchema=True)
    
    return cast, crew, keywords, links, links_small, movies_metadata, ratings, ratings_small
