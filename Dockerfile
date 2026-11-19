FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY sql ./sql

EXPOSE 8000

# Non-root runtime user.
RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app
# Mount point of the persistent job-state volume; owned by the runtime user so
# job.json / package commits can be fsynced without running as root.
RUN mkdir -p /data && chown -R appuser:appuser /data
USER appuser

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
