FROM python:2.7-slim-buster

WORKDIR /app
COPY . /app

# Installing requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Setting up entrypoint
WORKDIR /
CMD ["python", "-m", "app.api"]

EXPOSE 5000