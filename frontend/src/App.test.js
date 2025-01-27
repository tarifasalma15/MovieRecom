import React from 'react';
import { render, screen } from '@testing-library/react';
import App from './App';
// comments

test('renders learn react link', () => {
  render(<App />);
  const linkElement = screen.getByText(/movie recommendation system/i);
  expect(linkElement).toBeInTheDocument();
});
