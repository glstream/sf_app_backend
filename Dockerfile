# syntax=docker/dockerfile:1

FROM python:3.11-slim as base

WORKDIR /code

# Install dependencies
FROM base as builder
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Copy application code
FROM base
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY . .

EXPOSE 3100

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD curl --fail http://localhost:3100/health || exit 1

CMD ["gunicorn", "-c", "gunicorn.conf.py", "main:app"]
