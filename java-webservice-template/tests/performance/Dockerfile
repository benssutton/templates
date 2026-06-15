FROM grafana/k6:latest
# k6 runs as the non-root 'k6' user (uid/gid 12345). Own the scripts dir so
# handleSummary can write attribution.json next to the scripts (otherwise the
# root-owned WORKDIR denies the write).
COPY --chown=12345:12345 . /scripts
WORKDIR /scripts
# The app under test serves HTTPS with a self-signed dev cert, so skip TLS
# verification for every k6 run launched from this image (CI and local docs).
ENV K6_INSECURE_SKIP_TLS_VERIFY=true
