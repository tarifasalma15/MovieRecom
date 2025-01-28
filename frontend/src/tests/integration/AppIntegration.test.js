import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import App from '../../App';

test('renders full app with MovieSelect component', () => {
  render(<App />);
  const inputElement = screen.getByPlaceholderText(/type a movie name/i);
  expect(inputElement).toBeInTheDocument();
});

//Vérifie que MovieSelect s’affiche dans l’application
test('renders MovieSelect component in App', () => {
  render(<App />);
  const inputElement = screen.getByPlaceholderText(/type a movie name/i);
  expect(inputElement).toBeInTheDocument();
});

//Vérifie la sélection d’un film et son affichage global
test('selects a movie and updates the display in App', () => {
  render(<App />);
  const inputElement = screen.getByPlaceholderText(/type a movie name/i);
  fireEvent.change(inputElement, { target: { value: 'Inception' } });

  const selectedMovie = screen.getByText(/You selected: Inception/i);
  expect(selectedMovie).toBeInTheDocument();
});
