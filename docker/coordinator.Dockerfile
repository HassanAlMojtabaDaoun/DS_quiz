# Coordinator (metadata service): placement, redundancy, repair planning, search index.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
# zfec is optional (erasure coding falls back to the built-in codec).
RUN pip install --no-cache-dir zfec || echo "zfec not installed: built-in Reed-Solomon codec"
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --create-home --uid 10001 appuser

COPY observability/ ./observability/
COPY coordinator/ ./coordinator/
RUN mkdir -p /data && chown -R appuser:appuser /app /data
USER appuser

EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,sys; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)" || exit 1
CMD ["uvicorn", "coordinator.main:app", "--host", "0.0.0.0", "--port", "8000"]
