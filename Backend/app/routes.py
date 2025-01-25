from fastapi import APIRouter, HTTPException
from app.database import get_movies , get_movie_genres_by_title , get_recommendations_by_genre
from app.recommender import recommend_by_title, recommend_by_genre

router = APIRouter()

@router.get("/movies")
def fetch_movies():
    """
    Fetch the list of movies for the user to choose from.
    """
    movies = get_movies()
    return {"movies": movies}

@router.post("/recommend")
def recommend(input_value: str, input_type: str = "title"):
    """
    API endpoint for recommendations.
    """
    try:
        if input_type == "title":
            recommendations = recommend_by_title(input_value)
        elif input_type == "genre":
            recommendations = recommend_by_genre(input_value)
        else:
            raise ValueError("Invalid input type.")
        
        if not recommendations:
            return {"message": "No recommendations found."}
        return {"input_value": input_value, "recommendations": recommendations}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))