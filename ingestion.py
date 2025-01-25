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
    credits = spark.read.csv("raw_datasets/credits.csv", header=True, inferSchema=True)
    keywords = spark.read.csv("raw_datasets/keywords.csv", header=True, inferSchema=True)
    links = spark.read.csv("raw_datasets/links.csv", header=True, inferSchema=True)
    links_small = spark.read.csv("raw_datasets/links_small.csv", header=True, inferSchema=True)
    movies_metadata = spark.read.csv("raw_datasets/movies_metadata.csv", header=True, inferSchema=True)
    ratings = spark.read.csv("raw_datasets/ratings.csv", header=True, inferSchema=True)
    ratings_small = spark.read.csv("raw_datasets/ratings_small.csv", header=True, inferSchema=True)
    
    return credits, keywords, links, links_small, movies_metadata, ratings, ratings_small
