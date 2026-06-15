FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# Mint a fresh self-signed cert inside the image (the repo's *.pem are gitignored
# and excluded via .dockerignore, so they never ship in the build context).
RUN python certs/generate_self_signed_cert.py
# Serve over HTTPS on the default port, exactly like `python main.py` locally:
# main.py reads ssl_keyfile/ssl_certfile and server_port (443) from Settings.
CMD ["python", "main.py"]
