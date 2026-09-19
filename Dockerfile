FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data

EXPOSE 8000

# Default to the web UI. The CLI is still available by overriding the command:
#   docker compose run --rm cli --tld com,net
CMD ["uvicorn", "domain_scout.web:app", "--host", "0.0.0.0", "--port", "8000"]
