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


from pyspark.sql.functions import col, explode, from_json, to_date, year, month, dayofmonth, dayofweek, concat_ws
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType


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
        col("cast.name").alias("name"),
        col("cast.order").alias("order")
    )
    crew = crew_exploded.select(
        col("tmdbId"),
        col("crew.id").alias("crew_id"),
        col("crew.name").alias("name"),
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


from pyspark.sql.functions import col, udf, lit, when, concat_ws
from pyspark.sql.types import StringType, ArrayType
import ast

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
    ratings = ratings.withColumn("date", to_date((col("timestamp") / 1000).cast("timestamp")))
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
    ratings_small = ratings_small.withColumn("date", to_date((col("timestamp") / 1000).cast("timestamp")))
    ratings_small = ratings_small.withColumn("year", year(col("date")))
    ratings_small = ratings_small.withColumn("month", month(col("date")))
    ratings_small = ratings_small.withColumn("day", dayofmonth(col("date")))
    ratings_small = ratings_small.withColumn("weekday", dayofweek(col("date")))

    # Drop the original timestamp column
    ratings_small = ratings_small.drop("timestamp")
    
    return ratings_small


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import desc

# import pandas as pd
# import ast

# def transform_credits(credits: pd.DataFrame) -> pd.DataFrame:
#     """
#     To transform the credits dataset.
#     """

#     # Create a Spark session
#     spark = SparkSession \
#         .builder \
#         .config("spark.driver.host", "localhost") \
#         .appName('Transformation') \
#         .master("local[*]") \
#         .getOrCreate()

#     # Convert the 'cast' & 'crew' columns from string to actual lists of dictionaries
#     credits.cast = credits.cast.apply(ast.literal_eval)
#     credits.crew = credits.crew.apply(ast.literal_eval)

#     # Create a new DataFrame by exploding the 'cast' & 'crew' list
#     cast_exploded = credits.explode('cast', ignore_index=True)
#     crew_exploded = credits.explode('crew', ignore_index=True)

#     # Now, expand the 'cast' & 'crew' dictionaries into individual columns
#     cast_credits = pd.json_normalize(cast_exploded.cast)
#     crew_credits = pd.json_normalize(crew_exploded.crew)

#     # Concatenate the expanded cast DataFrame with the 'id' column
#     cast = pd.concat([cast_credits, cast_exploded['id']], axis=1)
#     crew = pd.concat([crew_credits, crew_exploded['id']], axis=1)

#     # Rename columns
#     cast.columns.values[4] = 'actor_id'
#     crew.columns.values[3] = 'crew_id'

#     cast.columns.values[-1] = 'tmdbId'
#     crew.columns.values[-1] = 'tmdbId'

#     # Convert data types
#     cast[['cast_id', 'gender', 'actor_id', 'order']] = cast[['cast_id', 'gender', 'actor_id', 'order']].astype('Int64')
#     crew[['crew_id', 'gender']] = crew[['crew_id', 'gender']].astype('Int64')
    
#     return cast, crew

# def transform_keywords(keywords: pd.DataFrame) -> pd.DataFrame:
#     """
#     To transform the keywords dataset.
#     """

#     # Convert the 'keywords' column from string to actual list of dictionaries
#     keywords.keywords = keywords.keywords.apply(ast.literal_eval)

#     # Create a new DataFrame by exploding the 'keywords' list
#     keywords_exploded = keywords.explode('keywords', ignore_index=True)

#     # Expand the 'keywords' dictionaries into individual columns
#     keywords = pd.json_normalize(keywords_exploded.keywords)

#     # Concatenate the expanded keywords DataFrame with the 'id' column
#     keywords = pd.concat([keywords, keywords_exploded['id']], axis=1)

#     # Rename columns
#     keywords.columns.values[1] = 'keyword_id'
#     keywords.columns.values[-1] = 'tmdbId'

#     # Convert data types
#     keywords[['keyword_id']] = keywords[['keyword_id']].astype('Int64')

#     return keywords

# def transform_links(links: pd.DataFrame) -> pd.DataFrame:
#     """
#     To transform the links dataset.
#     """

#     # Convert the 'imdbId' column to integer
#     links['imdbId'] = links['imdbId'].astype('Int64')

#     return links

# def transform_links_small(links_small: pd.DataFrame) -> pd.DataFrame:
#     """
#     To transform the links_small dataset.
#     """

#     # Convert the 'imdbId' column to integer
#     links_small['imdbId'] = links_small['imdbId'].astype('Int64')

#     return links_small

# # Define a function to safely parse the string as a dictionary (for the movies_metadata dataset)
# def safe_literal_eval(val: str) -> dict:
#     if pd.notnull(val):
#         if isinstance(val, str):
#             try:
#                 return ast.literal_eval(val)
#             except (ValueError, SyntaxError):
#                 return None
#         elif isinstance(val, dict):
#             return val
#     return None

# def transform_movies_metadata(movies_metadata: pd.DataFrame) -> pd.DataFrame:
#     """
#     To transform the movies_metadata dataset.
#     """

#     # Parse the concerned columns
#     movies_metadata['belongs_to_collection'] = movies_metadata['belongs_to_collection'].apply(safe_literal_eval)
#     movies_metadata['genres'] = movies_metadata['genres'].apply(safe_literal_eval)
#     movies_metadata['production_companies'] = movies_metadata['production_companies'].apply(safe_literal_eval)
#     movies_metadata['production_countries'] = movies_metadata['production_countries'].apply(safe_literal_eval)
#     movies_metadata['spoken_languages'] = movies_metadata['spoken_languages'].apply(safe_literal_eval)

#     # Extract only the names and store them as a list
#     movies_metadata['belongs_to_collection'] = movies_metadata['belongs_to_collection'].apply(
#         lambda x: x['name'] if isinstance(x, dict) and 'name' in x else 'NaN'
#     )
#     movies_metadata['genres'] = movies_metadata['genres'].apply(
#         lambda genre_list: [genre['name'] for genre in genre_list] if isinstance(genre_list, list) else 'NaN'
#     )
#     movies_metadata['production_companies'] = movies_metadata['production_companies'].apply(
#         lambda prod_companies_list: [prod_company['name'] for prod_company in prod_companies_list] if isinstance(prod_companies_list, list) else 'NaN'
#     )
#     movies_metadata['production_countries'] = movies_metadata['production_countries'].apply(
#         lambda prod_countries_list: [prod_country['name'] for prod_country in prod_countries_list] if isinstance(prod_countries_list, list) else 'NaN'
#     )
#     movies_metadata['spoken_languages'] = movies_metadata['spoken_languages'].apply(
#         lambda spoken_languages_list: [language['name'] for language in spoken_languages_list] if isinstance(spoken_languages_list, list) else 'NaN'
#     )

#     movies_metadata.columns.values[5] = 'tmdbId'
#     movies_metadata.columns.values[6] = 'imdbId'

#     return movies_metadata

# def transform_ratings(ratings: pd.DataFrame) -> pd.DataFrame:
#     """
#     To transform the ratings dataset.
#     """

#     # Convert the timestamp to a datetime object
#     ratings.timestamp = pd.to_datetime(ratings.timestamp, unit='s')

#     # Formate datetime in string with the format '%Y-%m-%d'
#     ratings.timestamp = ratings.timestamp.dt.strftime('%Y-%m-%d')

#     # Year
#     ratings['year'] = pd.to_datetime(ratings.timestamp).dt.year

#     # Month
#     ratings['month'] = pd.to_datetime(ratings.timestamp).dt.month

#     # Day
#     ratings['day'] = pd.to_datetime(ratings.timestamp).dt.day

#     # Weekday
#     ratings['weekday'] = pd.to_datetime(ratings.timestamp).dt.day_name()

#     # Delete the timestamp column
#     ratings.drop(columns='timestamp', inplace=True)

#     return ratings

# def transform_ratings_small(ratings_small: pd.DataFrame) -> pd.DataFrame:
#     """
#     To transform the ratings_small dataset.
#     """

#     # Convert the timestamp to a datetime object
#     ratings_small.timestamp = pd.to_datetime(ratings_small.timestamp, unit='s')

#     # Formate datetime in string with the format '%Y-%m-%d'
#     ratings_small.timestamp = ratings_small.timestamp.dt.strftime('%Y-%m-%d')

#     # Year
#     ratings_small['year'] = pd.to_datetime(ratings_small.timestamp).dt.year

#     # Month
#     ratings_small['month'] = pd.to_datetime(ratings_small.timestamp).dt.month

#     # Day
#     ratings_small['day'] = pd.to_datetime(ratings_small.timestamp).dt.day

#     # Weekday
#     ratings_small['weekday'] = pd.to_datetime(ratings_small.timestamp).dt.day_name()

#     # Delete the timestamp column
#     ratings_small.drop(columns='timestamp', inplace=True)

#     return ratings_small
