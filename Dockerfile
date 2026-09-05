FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg curl gosu tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 1000 babel && useradd -u 1000 -g babel -M -d /app babel

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Stamped into the image so a running container can be matched to the commit
# it was built from — `latest` is republished on every push and the semantic
# version rarely moves.
ARG BABEL_BUILD=source
ENV BABEL_BUILD=${BABEL_BUILD}

COPY src/ ./src/
# Operator tools the README tells people to run inside the container
# (scripts/check_dub_lookup.py, scripts/repro_hung_mount.py). Without this the
# documented `docker exec babel python scripts/...` fails on a missing file.
COPY scripts/ ./scripts/
COPY entrypoint.sh /entrypoint.sh
RUN mkdir -p /app/data && chmod +x /entrypoint.sh && chown -R babel:babel /app

EXPOSE 8686
# --max-time bounds curl itself so a half-open connection to a stalled loop
# fails as a healthcheck rather than lingering until Docker kills it.
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -fsS --max-time 8 http://localhost:8686/api/health || exit 1

# tini is PID 1 so that the container has a real init: it reaps orphaned
# processes and forwards signals. Note this is defense in depth only — it does
# NOT address the zombie leak this hardening was written for, because those
# children's parent (uvicorn) never died, so they were never reparented to PID 1.
# See src/scanner/ffprobe.py and src/watchdog.py for the actual fix.
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/entrypoint.sh"]
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8686"]
