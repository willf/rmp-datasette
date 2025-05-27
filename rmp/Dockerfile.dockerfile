FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt

ENV PORT=8000
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "datasette:app", "--config", "python:datasette.gunicorn_config"]