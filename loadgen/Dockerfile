# Slim official Python base, pinned. Both machines build the same image.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY loadgen.py .

RUN useradd --create-home appuser
USER appuser

# One-shot benchmark tool: run it, it pushes load for LOADTEST_DURATION, then exits.
CMD ["python", "loadgen.py"]
