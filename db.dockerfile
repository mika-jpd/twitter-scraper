FROM python:3.11
LABEL authors="mikad"

# Install SQLite and pip
RUN apt-get update && \
    apt-get install -y sqlite3 && \
    pip install sqlite-web

# Set working directory
WORKDIR /
COPY db/ db/

# Run sqlite-web interface
CMD ["sqlite_web", "--host", "0.0.0.0", "--port", "8080", "db/accounts.db"]