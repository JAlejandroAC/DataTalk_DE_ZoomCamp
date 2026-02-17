FROM python:3.13.11-slim
# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code

ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml ./
RUN uv sync

COPY main.py . 
ENTRYPOINT ["uv", "run", "python", "main.py"]