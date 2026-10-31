FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY schemas ./schemas
COPY migrations ./migrations
RUN pip install --no-cache-dir -e '.[dev]'
COPY config ./config
COPY tests ./tests
RUN mkdir -p /app/outputs /app/data
ENTRYPOINT ["recap"]
CMD ["--help"]
