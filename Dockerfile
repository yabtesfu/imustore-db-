FROM python:3.12-slim

WORKDIR /app

# Install the package (dependency-free, so this stays small and fast).
COPY pyproject.toml README.md ./
COPY imustore ./imustore
RUN pip install --no-cache-dir .

# 6380 = RESP protocol, 9100 = Prometheus metrics.
EXPOSE 6380 9100
VOLUME ["/data"]

ENTRYPOINT ["python", "-m", "imustore.server"]
CMD ["--host", "0.0.0.0", "--port", "6380", "--path", "/data/immustore.db", "--metrics-port", "9100"]
