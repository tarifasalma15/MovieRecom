import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import App from '../../frontend/src/App';
import SelectMovie from '../../frontend/src/components/MovieSelect';

//Test de chargement de la page principale

describe('Home Page Loads', () => {
    it('should display the movie search input', () => {
      cy.visit('http://localhost:3000'); // Adaptez l'URL si nécessaire
      cy.get('input[placeholder="type a movie name"]').should('be.visible');
    });
  });
  
//Test de recherche et sélection de film

describe('Movie Search and Select', () => {
    it('should search for a movie and display suggestions', () => {
      cy.visit('http://localhost:3000');
      cy.get('input[placeholder="type a movie name"]').type('Incep');
      cy.contains('Inception').click(); // Supposez que "Inception" est dans les suggestions
      cy.get('div').contains('You selected: Inception').should('be.visible');
    });
  });
  

  //Test d'erreur ou de recherche vide
  describe('Movie Search - No Results', () => {
    it('should display a message if no movies are found', () => {
      cy.visit('http://localhost:3000');
      cy.get('input[placeholder="type a movie name"]').type('RandomMovieThatDoesNotExist');
      cy.contains('No movies found').should('be.visible'); // Ajoutez cette logique dans votre composant
    });
  });
  