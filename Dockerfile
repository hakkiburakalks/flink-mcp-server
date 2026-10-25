FROM python:3.13-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

ENV FLINK_REST_URL=http://host.docker.internal:8081
# stdio: `docker run -i --rm ...`; HTTP: add `--transport streamable-http --host 0.0.0.0` (needs FLINK_MCP_AUTH_TOKEN)
EXPOSE 8000
ENTRYPOINT ["flink-mcp-server"]
