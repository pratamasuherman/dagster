# Main Dagster + NLP — single container
# Berat karena torch (~800 MB), tapi semua dalam satu service.
# Main Dagster + NLP — single container
# Berat karena torch (~800 MB), tapi semua dalam satu service.
FROM python:3.11-slim

# Install system deps + build tools (dibutuhkan untuk compile beberapa Python deps)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    libpq-dev \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# HF_HOME di-volume agar cache model persist antar redeploy
# (meski container ephemeral, volume bisa di-attach sebagai PVC di Railway)
ENV HF_HOME=/root/.cache/huggingface
ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/app/.dagster_home

# Install Python deps (production only — tanpa [dev])
COPY pyproject.toml ./
RUN pip install --no-cache-dir -e "."

# Hapus build tools setelah install untuk memperkecil image
RUN apt-get purge -y --auto-remove gcc g++ libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy source
COPY . .

# Pre-warm model cache directory
RUN mkdir -p $HF_HOME

EXPOSE 3000

CMD ["bash", "start.sh"]
