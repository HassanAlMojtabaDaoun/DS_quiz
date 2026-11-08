# Generic image with every Python service (kept for convenience).
# docker-compose.yml uses one Dockerfile per service from docker/.
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir zfec || echo "zfec not installed: built-in codec"
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --create-home --uid 10001 appuser
COPY observability/ ./observability/
COPY coordinator/ ./coordinator/
COPY storage_node/ ./storage_node/
COPY gateway/ ./gateway/
COPY client/ ./client/
RUN mkdir -p /data && chown -R appuser:appuser /app /data
USER appuser
EXPOSE 8000
CMD ["uvicorn", "storage_node.main:app", "--host", "0.0.0.0", "--port", "8000"]
