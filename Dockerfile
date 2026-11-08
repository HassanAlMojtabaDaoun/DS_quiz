FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .

# zfec is optional: if it cannot be installed, the built-in Reed-Solomon
# codec (coordinator/erasure.py) is used instead.
RUN pip install --no-cache-dir zfec \
    || echo "zfec not installed: using the built-in Reed-Solomon codec"

RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --create-home --uid 10001 appuser

COPY coordinator/ ./coordinator/
COPY storage_node/ ./storage_node/
COPY client/ ./client/
COPY gateway/ ./gateway/

RUN mkdir -p /data \
    && chown -R appuser:appuser /app /data

USER appuser

EXPOSE 8000

CMD ["uvicorn", "storage_node.main:app", "--host", "0.0.0.0", "--port", "8000"]