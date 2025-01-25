from app.database import get_movie_genres_by_title
from app.utils import get_recommendations_by_genre
def recommend_by_title(movie_title):
    """
    Recommend movies based on a movie title.
    """
    genres = get_movie_genres_by_title(movie_title)
    if not genres:
        return []
    return get_recommendations_by_genre(genres)

def recommend_by_genre(genre):
    """
    Recommend movies based on a genre.
    """
    return get_recommendations_by_genre([genre])