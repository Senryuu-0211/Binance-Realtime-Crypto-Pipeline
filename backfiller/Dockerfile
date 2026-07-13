# Slim official Python base, pinned. Both machines build the same image.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backfill.py .

RUN useradd --create-home appuser
USER appuser

# Long-running service (loop mode): scans for trade_id gaps and heals them.
# Set BACKFILL_ONCE=1 to run a single pass and exit (used by `make backfill`).
CMD ["python", "backfill.py"]
