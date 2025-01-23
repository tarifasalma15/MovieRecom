import pandas as pd

def query_cast_by_actor(cast: pd.DataFrame, actor_name: str) -> pd.DataFrame:
    """
    To query the cast dataset by actor name.
    """
    return cast.filter(cast["name"] == actor_name)

def query_movies_by_genre(movies_metadata: pd.DataFrame, genre: str) -> pd.DataFrame:
    """
    To query the movies_metadata dataset by genre.
    """
    return movies_metadata.filter(
            movies_metadata["genres"].contains(genre)
        )