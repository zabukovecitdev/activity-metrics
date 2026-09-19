FROM python:3.14-slim

WORKDIR /app

# Copy only packaging metadata first for better layer caching
COPY pyproject.toml poetry.lock README.md ./
COPY src ./src

# pip can build/install the project directly via its poetry-core backend,
# so there's no need to install Poetry itself inside the image.
RUN pip install --no-cache-dir .

CMD ["collector"]
