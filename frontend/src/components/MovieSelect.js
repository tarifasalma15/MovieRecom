// src/components/
import React, { useState } from 'react';

const SelectMovie = () => {
  const [movie, setMovie] = useState('');

  const handleSelectChange = (e) => {
    setMovie(e.target.value);
  };

  return (
    <div>
      <h1>Movie Recommendation System</h1>
      <p>Type or select a movie to get recommendations:</p>
      <input
      list="movies"
      type="text"
      placeholder="Type a movie name..."
      value={movie}
      onChange={handleSelectChange}
    />
    <datalist id="movies">
      <option value="Inception"></option>
      <option value="The Matrix"></option>
      <option value="Titanic"></option>
      <option value="Avatar"></option>
      <option value="The Dark Knight"></option>
    </datalist>
        </div>
  );
};

export default SelectMovie;
