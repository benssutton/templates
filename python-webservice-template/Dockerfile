# Python 3.12: the version CI tests the service against (.github/workflows/service-python.yml).
FROM python:3.12.16-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b
WORKDIR /app
# Exact versions: requirements.txt is the lock compiled from requirements.in; test
# and lint tooling lives in requirements-dev.txt and never enters the image.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app
COPY . .
USER 10001
# Unprivileged port inside the container; compose publishes it as 443. TLS
# material is mounted at runtime (./certs in compose), never baked into a layer.
ENV SERVER_PORT=8443
EXPOSE 8443
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=6 \
    CMD ["python", "-c", "import ssl, urllib.request; urllib.request.urlopen('https://localhost:8443/health/live', context=ssl._create_unverified_context(), timeout=4)"]
CMD ["python", "main.py"]
