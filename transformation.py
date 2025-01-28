# 
# This file is used to handle data transformation by transforming the datasets.
#
# This file contains 6 functions:
# 
# transform_credits(credits: pd.DataFrame) -> pd.DataFrame: To transform the credits dataset.
# transform_keywords(keywords: pd.DataFrame) -> pd.DataFrame: To transform the keywords dataset.
# transform_links(links: pd.DataFrame) -> pd.DataFrame: To transform the links dataset.
# transform_links_small(links_small: pd.DataFrame) -> pd.DataFrame: To transform the links_small dataset.
# transform_movies_metadata(movies_metadata: pd.DataFrame) -> pd.DataFrame: To transform the movies_metadata dataset.
# transform_ratings(ratings: pd.DataFrame) -> pd.DataFrame: To transform the ratings dataset.
# transform_ratings_small(ratings_small: pd.DataFrame) -> pd.DataFrame: To transform the ratings_small dataset.
# 


from pyspark.sql.functions import col, explode, from_json, to_date, year, month, dayofmonth, dayofweek, concat_ws, udf, concat_ws
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType
import ast

# Define transformation functions for each dataset

def transform_credits(credits):
    """
    Transform the credits dataset: explode and normalize the cast and crew columns.
    """
    # Define schema for nested 'cast' and 'crew' JSON columns
    cast_schema = ArrayType(
        StructType([
            StructField("cast_id", IntegerType(), True),
            StructField("character", StringType(), True),
            StructField("gender", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("order", IntegerType(), True),
        ])
    )
    crew_schema = ArrayType(
        StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("job", StringType(), True),
        ])
    )
    
    # Parse JSON strings to struct
    credits = credits.withColumn("cast", from_json(col("cast"), cast_schema))
    credits = credits.withColumn("crew", from_json(col("crew"), crew_schema))
    
    # Explode the arrays
    cast_exploded = credits.select(col("id").alias("tmdbId"), explode(col("cast")).alias("cast"))
    crew_exploded = credits.select(col("id").alias("tmdbId"), explode(col("crew")).alias("crew"))
    
    # Normalize nested fields
    cast = cast_exploded.select(
        col("tmdbId"),
        col("cast.cast_id").alias("cast_id"),
        col("cast.character").alias("character"),
        col("cast.gender").alias("gender"),
        col("cast.id").alias("actor_id"),
        col("cast.name").alias("actor_name"),
        col("cast.order").alias("order")
    )
    crew = crew_exploded.select(
        col("tmdbId"),
        col("crew.id").alias("crew_id"),
        col("crew.name").alias("crew_name"),
        col("crew.department").alias("department"),
        col("crew.job").alias("job")
    )
    
    return cast, crew


def transform_keywords(keywords):
    """
    Transform the keywords dataset: explode and normalize the keywords column.
    """
    # Define schema for nested 'keywords' JSON column
    keywords_schema = ArrayType(
        StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
    )
    
    # Parse JSON strings to struct
    keywords = keywords.withColumn("keywords", from_json(col("keywords"), keywords_schema))
    
    # Explode the arrays
    keywords_exploded = keywords.select(col("id").alias("tmdbId"), explode(col("keywords")).alias("keyword"))
    
    # Normalize nested fields
    keywords = keywords_exploded.select(
        col("tmdbId"),
        col("keyword.id").alias("keyword_id"),
        col("keyword.name").alias("keyword_name")
    )
    
    return keywords


# Define a UDF to safely parse the string as a dictionary
def safe_literal_eval(val):
    if val:
        if isinstance(val, str):
            try:
                return ast.literal_eval(val)
            except (ValueError, SyntaxError):
                return None
        elif isinstance(val, dict):
            return val
    return None

safe_literal_eval_udf = udf(safe_literal_eval, StringType())

# Define a UDF to extract 'name' from a dictionary or return 'NaN'
def extract_name(val):
    if isinstance(val, dict) and 'name' in val:
        return val['name']
    return "NaN"

extract_name_udf = udf(extract_name, StringType())

# Define a UDF to extract names from a list of dictionaries
def extract_names_from_list(val):
    if isinstance(val, list):
        return [item['name'] for item in val if 'name' in item]
    return []

extract_names_udf = udf(extract_names_from_list, ArrayType(StringType()))

def transform_movies_metadata(movies_metadata):
    # Apply transformations
    movies_metadata = movies_metadata.withColumn(
        "belongs_to_collection",
        extract_name_udf(safe_literal_eval_udf(col("belongs_to_collection")))
    )

    movies_metadata = movies_metadata.withColumn(
        "genres",
        extract_names_udf(safe_literal_eval_udf(col("genres")))
    )

    movies_metadata = movies_metadata.withColumn(
        "production_companies",
        extract_names_udf(safe_literal_eval_udf(col("production_companies")))
    )

    movies_metadata = movies_metadata.withColumn(
        "production_countries",
        extract_names_udf(safe_literal_eval_udf(col("production_countries")))
    )

    movies_metadata = movies_metadata.withColumn(
        "spoken_languages",
        extract_names_udf(safe_literal_eval_udf(col("spoken_languages")))
    )

    # Convert array columns to string columns
    movies_metadata = movies_metadata.withColumn(
        "genres", concat_ws(", ", col("genres"))
    )

    movies_metadata = movies_metadata.withColumn(
        "production_companies", concat_ws(", ", col("production_companies"))
    )

    movies_metadata = movies_metadata.withColumn(
        "production_countries", concat_ws(", ", col("production_countries"))
    )

    movies_metadata = movies_metadata.withColumn(
        "spoken_languages", concat_ws(", ", col("spoken_languages"))
    )

    # Rename columns
    movies_metadata = movies_metadata.withColumnRenamed("id", "tmdbId").withColumnRenamed("imdb_id", "imdbId")

    return movies_metadata
    # Save the DataFrame as a CSV
    # movies_metadata.write.csv("datasets/movies_metadata.csv", header=True, mode='overwrite')


def transform_links(links):
    """
    Transform the links dataset: ensure tmdbId is in integer format.
    """
    links = links.withColumn("tmdbId", col("tmdbId").cast("Int"))
    return links


def transform_links_small(links_small):
    """
    Transform the links_small dataset: ensure tmdbId is in integer format.
    """
    links_small = links_small.withColumn("tmdbId", col("tmdbId").cast("Int"))
    return links_small


def transform_ratings(ratings):
    """
    Transform the ratings dataset: parse timestamps and add date-related fields.
    """
    ratings = ratings.withColumn("date", to_date((col("timestamp")).cast("timestamp")))
    ratings = ratings.withColumn("year", year(col("date")))
    ratings = ratings.withColumn("month", month(col("date")))
    ratings = ratings.withColumn("day", dayofmonth(col("date")))
    ratings = ratings.withColumn("weekday", dayofweek(col("date")))

    # Drop the original timestamp column
    ratings = ratings.drop("timestamp")
    
    return ratings


def transform_ratings_small(ratings_small):
    """
    Transform the ratings_small dataset: parse timestamps and add date-related fields.
    """
    ratings_small = ratings_small.withColumn("date", to_date((col("timestamp")).cast("timestamp")))
    ratings_small = ratings_small.withColumn("year", year(col("date")))
    ratings_small = ratings_small.withColumn("month", month(col("date")))
    ratings_small = ratings_small.withColumn("day", dayofmonth(col("date")))
    ratings_small = ratings_small.withColumn("weekday", dayofweek(col("date")))

    # Drop the original timestamp column
    ratings_small = ratings_small.drop("timestamp")
    
    return ratings_small
