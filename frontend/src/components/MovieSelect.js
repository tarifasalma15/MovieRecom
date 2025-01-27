// src/components/MovieSelect.js
import React, { useState } from 'react';
import './MovieSelect.css'; // Importation du fichier CSS

const SelectMovie = () => {
  const [movie, setMovie] = useState('');

  const handleSelectChange = (e) => {
    setMovie(e.target.value);
  };

  return (
    <div className="container">
      <h1 className="title">Movie Recommendation System</h1>
      <p className="subtitle">Type or select a movie to get recommendations:</p>

      <input
        className="input"
        list="movies"
        type="text"
        placeholder="Type a movie name..."
        value={movie}
        onChange={handleSelectChange}
      />

      <datalist id="movies">
        <option value="Inception" />
        <option value="The Matrix" />
        <option value="Titanic" />
        <option value="Avatar" />
        <option value="The Dark Knight" />
      </datalist>

      {movie && (
        <p className="selected-movie">
          <strong>You selected:</strong> {movie}
        </p>
      )}
    </div>
  );
};

export default SelectMovie;
