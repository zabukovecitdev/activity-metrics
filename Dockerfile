FROM python:3.14-slim
COPY --from=ghcr.io/astral-sh/uv:0.11.28 /uv /bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY src ./src

# Installs the versions in uv.lock, so a new PyPI release cannot break the build.
# No dev or experiments groups, and the image's own Python, never a downloaded one.
RUN UV_PYTHON_DOWNLOADS=never uv sync --frozen --no-default-groups --no-editable --no-cache
ENV PATH=/app/.venv/bin:$PATH
