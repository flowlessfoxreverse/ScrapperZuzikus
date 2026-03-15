FROM python:3.11-slim

WORKDIR /app

# System deps for dnspython + asyncpg compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps first (layer-cached unless pyproject.toml changes)
# Create a minimal app stub so hatchling can resolve the package before source arrives
COPY pyproject.toml .
RUN mkdir -p app && touch app/__init__.py
RUN pip install --no-cache-dir ".[dev]"

# Copy source (replaces the stub)
COPY . .

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
