FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt

ENV PORT=8001
EXPOSE 8001

CMD ["datasette", "serve", "risk-management-plans.db", "--setting", "sql_time_limit_ms", "2500", "--metadata", "metadata.json", "--plugins-dir", "plugins", "--template-dir", "templates", "--static", "static:static"]
