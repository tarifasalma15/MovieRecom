import sqlite3
import os

DB_PATH = os.path.join("..", "movies.db")

def get_recommendations_by_genre(genres):
    """
    Get movie recommendations based on a list of genres.
    """
    conn = sqlite3.connect(DB_PATH)

    # Prepare a dynamic SQL query to match any genre
    query = f"""
    SELECT movieId, title 
    FROM movies 
    WHERE {" OR ".join(["genres LIKE ?" for _ in genres])}
    LIMIT 5
    """

    # Add wildcards for partial matches
    genre_params = [f"%{genre}%" for genre in genres]

    # Execute the query
    recommendations = conn.execute(query, genre_params).fetchall()
    conn.close()
    return [{"movieId": rec[0], "title": rec[1]} for rec in recommendations]

