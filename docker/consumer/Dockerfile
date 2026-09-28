FROM python:3.14-slim

WORKDIR /app

# Copy only packaging metadata first for better layer caching
COPY pyproject.toml README.md ./
COPY src ./src

# pip can build/install the project directly via its hatchling backend,
# so there's no need to install uv itself inside the image.
RUN pip install --no-cache-dir .

CMD ["metrics-writer"]
