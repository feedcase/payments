FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
WORKDIR /app

COPY requirements.lock.txt .
RUN pip install --no-cache-dir -r requirements.lock.txt

COPY alembic.ini ./
COPY src ./src
COPY scripts ./scripts

RUN useradd --create-home --uid 1000 app
USER app

EXPOSE 8000
CMD ["gunicorn", "app.main:create_app()", "-k", "uvicorn.workers.UvicornWorker", "-w", "2", "-b", "0.0.0.0:8000"]
