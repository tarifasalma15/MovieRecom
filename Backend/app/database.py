import sqlite3
import os
from app.utils import get_recommendations_by_genre

DB_PATH = os.path.join("..", "movies.db")  # SQLite database path

def get_movies():
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT movieId, title , genres FROM movies LIMIT 100"
    movies = conn.execute(query).fetchall()
    conn.close()
    return [{"movieId": movie[0], "title": movie[1] , "genres": movie[2]} for movie in movies]

def get_movie_genres(movie_id):
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT genres FROM movies WHERE movieId = ?"
    result = conn.execute(query, (movie_id,)).fetchone()
    conn.close()
    return result[0] if result else None

def get_movie_genres_by_title(title):
    """
    Get the genres of a movie by its title, split into individual genres.
    """
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT genres FROM movies WHERE title = ?"
    result = conn.execute(query, (title,)).fetchone()
    conn.close()
    if result:
        # Split genres by comma and strip whitespace
        genres = result[0].split(",") if result[0] else []
        return [genre.strip() for genre in genres]
    return None
