# 
# This file is used to handle data transformation by transforming the datasets.
#
# This file contains 1 function:
# 
# transform_merge(): To merge all the datasets into a single DataFrame
# 


from pyspark.sql.functions import collect_list, avg, count, col, expr
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType


def transform_merge(cast, crew, keywords, links, links_small, movies_metadata, ratings, ratings_small):
    """
    Merge all the datasets into a single DataFrame.
    """
    # Drop unnecessary columns
    to_delete_in_cast = ["cast_id", "actor_id", "order"]
    cast = cast.drop(*to_delete_in_cast)
    # Change the column names
    cast = cast.withColumnRenamed("name", "actor_name")

    to_delete_in_crew = ["crew_id"]
    crew = crew.drop(*to_delete_in_crew)
    # Change the column names
    crew = crew.withColumnRenamed("name", "crew_name")

    to_delete_in_keywords = ["keyword_id"]
    keywords = keywords.drop(*to_delete_in_keywords)

    to_delete_in_links = ["imdbId"]
    links = links.drop(*to_delete_in_links)

    to_delete_in_links_small = ["imdbId"]
    links_small = links_small.drop(*to_delete_in_links_small)

    to_delete_in_movies_metadata = ["homepage", "poster_path", "status", "tagline", "title", "video"]
    movies_metadata = movies_metadata.drop(*to_delete_in_movies_metadata)

    to_delete_in_ratings = ["date"]
    ratings = ratings.drop(*to_delete_in_ratings)

    to_delete_in_ratings_small = ["date"]
    ratings_small = ratings_small.drop(*to_delete_in_ratings_small)

    # Transform the datasets: Gather the lines with the same 'tmdbId' into a single line (list of values)
    cast = cast.groupBy("tmdbId").agg(
                collect_list("character").alias("character"),
                collect_list('gender').alias('gender'),
                collect_list('actor_name').alias('actor_name'))
    
    crew = crew.groupBy("tmdbId").agg(
                collect_list("crew_name").alias("crew_name"),
                collect_list("department").alias("department"),
                collect_list("job").alias("job"))
    
    df = movies_metadata.join(cast, "tmdbId", "left")
    df = df.join(crew, "tmdbId", "left")
    
    df = df.join(keywords.groupBy("tmdbId").agg(
                collect_list("keyword_name").alias("keyword_name")), "tmdbId", "left")
    
    # Add links and links_small together
    # print(links.count())
    links = links.union(links_small)
    # print(links.count())

    # print(ratings.count())
    ratings = ratings.union(ratings_small)
    # print(ratings.count())

    # Aggregate the ratings
    aggregated_ratings = ratings.groupBy("movieId").agg(
        avg("rating").alias("average_rating"),
        count("rating").alias("rating_count")
    )

    ratings = aggregated_ratings.join(links, "movieId", "left")

    ratings = ratings.drop("movieId")
    ratings = ratings.withColumnRenamed("tmdbId", "tmdb_Id")

    # Add the aggregated ratings to df
    df = df.join(ratings, df["tmdbId"] == ratings["tmdb_Id"], "left")

    col_to_drop = ["imdbId", "tmdb_Id"]

    df = df.drop(*col_to_drop)

    df = df.withColumn('genres', col('genres').cast('string')) \
           .withColumn('character', col('character').cast('string')) \
           .withColumn('gender', col('gender').cast('string')) \
           .withColumn('actor_name', col('actor_name').cast('string')) \
           .withColumn('crew_name', col('crew_name').cast('string')) \
           .withColumn('department', col('department').cast('string')) \
           .withColumn('job', col('job').cast('string')) \
           .withColumn('keyword_name', col('keyword_name').cast('string')) \
           .withColumn('production_companies', col('production_companies').cast('string')) \
           .withColumn('production_countries', col('production_countries').cast('string')) \
           .withColumn('spoken_languages', col('spoken_languages').cast('string'))
    

    # Drop duplicates
    df = df.dropDuplicates()

    # Percentage of missing values
    # print("Percentage of missing values in each column:")
    # for column in df.columns:
    #     missing = df.filter(df[column].isNull()).count()
    #     print(f"{column}: {missing/df.count()*100:.2f}%")
    
    return df
