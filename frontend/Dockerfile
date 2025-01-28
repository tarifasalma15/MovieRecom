# Image for frontend 
FROM node:18 AS frontend

# Working directory for the frontend
WORKDIR /app/frontend

#Copy files of frontend 
COPY ./package.json ./package-lock.json /app/frontend/

#Install dependencies frontend
RUN npm install 

# Copy the rest of the React app
COPY . /app/frontend/

# Expose port for React app
EXPOSE 3000

# Run React app
CMD ["npm", "start"]