FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt

ENV PORT=8001
CMD ["datasette", "serve", "--host", "0.0.0.0", "--port", "8001", "data/risk-management-plans.db"]
EXPOSE 8001