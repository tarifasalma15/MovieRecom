import React from 'react';
import { render, screen } from '@testing-library/react';
import App from '../../App';

test('renders full app with MovieSelect component', () => {
  render(<App />);
  const inputElement = screen.getByPlaceholderText(/type a movie name/i);
  expect(inputElement).toBeInTheDocument();
});
