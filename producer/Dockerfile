# Slim official Python base, pinned. Both machines build the same image.
FROM python:3.12-slim

# Don't write .pyc files; stream stdout/stderr straight to docker logs (unbuffered).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install deps first (separate layer) so code edits don't re-trigger pip install.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Run as a non-root user (good container hygiene).
RUN useradd --create-home appuser
USER appuser

CMD ["python", "producer.py"]
