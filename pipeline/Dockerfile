FROM python:3.11-slim-bookworm

# PySpark requires a JVM; create a stable JAVA_HOME symlink for both amd64 and arm64
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/default-java

ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /pipeline

# Install dependencies before copying source for better layer caching.
# pyspark is large; this layer is reused as long as pyproject.toml/uv.lock don't change.
COPY pyproject.toml uv.lock ./
RUN uv sync --group dev --no-install-project --frozen

# conftest.py adds dlt/ to sys.path; no package install needed.
COPY dlt/ dlt/
COPY tests/ tests/

CMD ["uv", "run", "--no-project", "pytest", "-v"]
