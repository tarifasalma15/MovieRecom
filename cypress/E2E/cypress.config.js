const { defineConfig } = require('cypress');

module.exports = defineConfig({
  e2e: {
    baseUrl: 'http://localhost:3000', // URL de votre application
    setupNodeEvents(on, config) {
      // Configurez vos écouteurs d'événements ici si nécessaire
    },
  },
});
