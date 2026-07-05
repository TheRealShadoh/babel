FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg curl gosu \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 1000 babel && useradd -u 1000 -g babel -M -d /app babel

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY entrypoint.sh /entrypoint.sh
RUN mkdir -p /app/data && chmod +x /entrypoint.sh && chown -R babel:babel /app

EXPOSE 8686
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8686/api/health || exit 1
ENTRYPOINT ["/entrypoint.sh"]
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8686"]
