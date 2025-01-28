import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import App from '../../App';
import SelectMovie from '../../components/MovieSelect';


// comments

test('renders learn react link', () => {
  render(<App />);
  const linkElement = screen.getByText(/movie recommendation system/i);
  expect(linkElement).toBeInTheDocument();
});

// Test for the selection of a film
test('updates the selected movie on input change', () => {
  render(<SelectMovie />);
  const inputElement = screen.getByPlaceholderText(/type a movie name/i);
  
  fireEvent.change(inputElement, { target: { value: 'Inception' } });

  const selectedMovieText = screen.getByText((content, element) => {
    const hasText = (node) => node.textContent === 'You selected: Inception';
    const nodeHasText = hasText(element);
    const childrenDoNotHaveText = Array.from(element.children || []).every(
      (child) => !hasText(child)
    );
    return nodeHasText && childrenDoNotHaveText;
  });

  expect(selectedMovieText).toBeInTheDocument();
});

//Vérifie que le champ de recherche est vide par défaut

test('renders input field empty by default', () => {
  render(<SelectMovie />);
  const inputElement = screen.getByPlaceholderText(/type a movie name/i);
  expect(inputElement.value).toBe('');
});
