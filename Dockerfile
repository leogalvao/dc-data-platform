# DC Data Platform - Production Container
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd --gid 1000 dcuser \
    && useradd --uid 1000 --gid dcuser --shell /bin/bash --create-home dcuser

# Set working directory
WORKDIR /app

# Copy application code first (needed for pyproject.toml to work)
COPY --chown=dcuser:dcuser . .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install ".[warehouse]"

# Create data directories
RUN mkdir -p /app/data/bronze /app/data/silver /app/data/gold /app/data/diamond \
    /app/data/quarantine /app/data/reports /app/data/checkpoints \
    && chown -R dcuser:dcuser /app/data

# Switch to non-root user
USER dcuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from config.settings import Settings; Settings()" || exit 1

# Default command - run the full pipeline
CMD ["python", "scripts/unified_pipeline.py"]
