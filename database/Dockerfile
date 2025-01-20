FROM python:3.10
LABEL authors="mikad"

# Install SQLite and pip
RUN apt-get update && \
    apt-get install -y sqlite3 && \
    pip install sqlite-web

# Set working directory
WORKDIR /data/db

# Run sqlite-web interface
CMD ["sqlite_web", "--host", "0.0.0.0", "--port", "8080", "/data/db/accounts.db"]