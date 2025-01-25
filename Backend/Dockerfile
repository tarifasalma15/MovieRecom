# Image for backend
FROM python:3.12-slim AS backend

#Working directory for backend 
WORKDIR /app/backend

#Copy files of backend 
COPY ./requirements.txt /app/backend/

#Install dependencies 
RUN pip install --no-cache-dir -r ./requirements.txt

# Copy the FastAPI app code
COPY . /app/backend/

# Expose port for FastAPI app
EXPOSE 8000

# Run FastAPI app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

