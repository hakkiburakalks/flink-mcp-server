FROM python:3.13-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

ENV FLINK_REST_URL=http://host.docker.internal:8081
# stdio transport: run with `docker run -i --rm ...`
ENTRYPOINT ["flink-mcp-server"]
