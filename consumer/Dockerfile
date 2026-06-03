# Slim official Python base, pinned. Both machines build the same image.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install deps first (separate layer) so code edits don't re-trigger pip install.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

RUN useradd --create-home appuser
USER appuser

CMD ["python", "consumer.py"]
