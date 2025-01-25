import pandas as pd
import sqlite3
import os

def preprocess_and_load():
    # Paths to datasets
    links_path = os.path.join("..", "datasets", "links", "links.csv")
    metadata_path = os.path.join("..", "datasets", "movies_metadata", "clean_movies_metadata.csv")


    # Load datasets
    links = pd.read_csv(links_path)
    metadata = pd.read_csv(metadata_path, low_memory=False)

    # Merge links and metadata on tmdbId
    links['tmdbId'] = links['tmdbId'].apply(pd.to_numeric, errors='coerce')  # Ensure tmdbId is numeric
    metadata['tmdbId'] = metadata['tmdbId'].apply(pd.to_numeric, errors='coerce')
    # Replace missing or null `genres` with "Unknown"
    metadata['genres'] = metadata['genres'].fillna("Unknown").str.strip()
    merged = pd.merge(links, metadata, on='tmdbId', how='inner')



    # Select relevant columns
    merged = merged[['movieId', 'title', 'genres']]
    
    # Save to SQLite database
    conn = sqlite3.connect(os.path.join("..", "movies.db"))
    merged.to_sql("movies", conn, if_exists="replace", index=False)
    conn.close()

    print("Preprocessed data saved to SQLite database!")

if __name__ == "__main__":
    preprocess_and_load()
