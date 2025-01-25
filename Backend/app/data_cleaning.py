import pandas as pd
import os

def clean_movies_metadata():
    # Define input and output file paths
    input_path = os.path.join("..", "datasets", "movies_metadata", "movies_metadata.csv")
    output_path = os.path.join("..", "datasets", "movies_metadata", "clean_movies_metadata.csv")

    try:
        # Load the data with proper quoting and error handling
        df = pd.read_csv(input_path, quotechar='"', skipinitialspace=True, on_bad_lines="skip")

        # Optional: Drop rows with missing critical values like title or genres
        df = df.dropna(subset=["title", "genres"])

        # Save the cleaned data
        df.to_csv(output_path, index=False)
        print(f"Cleaned data saved to: {output_path}")

    except Exception as e:
        print(f"Error cleaning the data: {e}")

if __name__ == "__main__":
    clean_movies_metadata()
