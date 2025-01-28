from flask import Flask, request, jsonify
import pandas as pd
import ast
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Initialize Flask app
app = Flask(__name__)

# Load the dataset
data = pd.read_csv(
    "ml_pipeline/ml_dataset_csv/part-00000-b1ab9cc5-c309-46ac-a12a-32c09484c4f9-c000.csv",
    on_bad_lines='skip',
    engine="python"  # Use the Python engine for parsing
)


# Function for safely evaluating list-like strings
def safe_eval(val):
    try:
        if isinstance(val, str):
            return " ".join(ast.literal_eval(val)) if val.startswith("[") and val.endswith("]") else val
        else:
            return ""
    except (ValueError, SyntaxError):
        return val


# Preprocess data
data['adult'] = data['adult'].fillna(False)
data['belongs_to_collection'] = data['belongs_to_collection'].apply(safe_eval)
data['budget'] = data['budget'].fillna(0)
data['genres'] = data['genres'].fillna("[]").apply(safe_eval)
data['original_language'] = data['original_language'].fillna("")
data['original_title'] = data['original_title'].fillna("")
data['overview'] = data['overview'].fillna("")
data['popularity'] = data['popularity'].fillna(0)
data['production_companies'] = data['production_companies'].apply(safe_eval)
data['production_countries'] = data['production_countries'].apply(safe_eval)
data['release_date'] = data['release_date'].fillna("")
data['revenue'] = data['revenue'].fillna(0)
data['runtime'] = data['runtime'].fillna(0)
data['spoken_languages'] = data['spoken_languages'].apply(safe_eval)
data['vote_average'] = data['vote_average'].fillna(0)
data['vote_count'] = data['vote_count'].fillna(0)
data['character'] = data['character'].fillna("")
data['actor_name'] = data['actor_name'].fillna("")
data['crew_name'] = data['crew_name'].fillna("")
data['keywords'] = data['keyword_name'].apply(safe_eval)
data['average_rating'] = data['average_rating'].fillna("")
data['rating_count'] = data['rating_count'].fillna(0)

# Create combined features for vectorization
data['combined_features'] = data['belongs_to_collection'].astype(str) * 4 + " " \
                            + data['genres'].astype(str) * 10 + " " \
                            + data['original_title'].astype(str) * 2 + " " \
                            + data['original_language'].astype(str) * 4 + " " \
                            + data['popularity'].astype(str) * 5 + " " \
                            + data['production_companies'].astype(str) * 4 + " " \
                            + data['production_countries'].astype(str) + " " \
                            + data['runtime'].astype(str) + " " \
                            + data['spoken_languages'].astype(str) * 3 + " " \
                            + data['actor_name'].astype(str) * 4 + " " \
                            + data['crew_name'].astype(str) * 2 + " " \
                            + data['keywords'].astype(str) * 4 + " " \
                            + data['average_rating'].astype(str) * 3 + " "


# Vectorize combined features using TF-IDF
vectorizer = TfidfVectorizer(stop_words='english')
tfidf_matrix = vectorizer.fit_transform(data['combined_features'])


# Function for generating movie recommendations
def recommend_movies(user_input, n_recommendations=5):
    user_combined_input = " ".join(f"{key}: {value}" for key, value in user_input.items())
    user_tfidf_vector = vectorizer.transform([user_combined_input])  # Transform user input to the same space
    cosine_sim = cosine_similarity(user_tfidf_vector, tfidf_matrix)  # Compare the user vector with movie vectors
    
    scores = list(enumerate(cosine_sim.flatten()))
    sorted_scores = sorted(scores, key=lambda x: x[1], reverse=True)
    
    input_movie_title = user_input.get('original_title', '')
    sorted_scores = [score for score in sorted_scores if data.iloc[score[0]]['original_title'] != input_movie_title]
    
    top_recommendations = sorted_scores[:n_recommendations]
    recommended_indices = [i[0] for i in top_recommendations]
    recommendation_scores = [i[1] for i in top_recommendations]
    
    recommendations = data.loc[recommended_indices, ['original_title', 'genres']]
    recommendations['cosine_similarity'] = recommendation_scores
    
    return recommendations.to_dict(orient='records')


# Flask route for receiving user input and providing recommendations
@app.route('/recommend', methods=['POST'])
def recommend():
    try:
        # Get user input from the request body
        user_input = request.json
        
        # Get movie recommendations
        recommendations = recommend_movies(user_input)
        
        return jsonify(recommendations), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Run the app
if __name__ == "__main__":
    app.run(debug=True)
