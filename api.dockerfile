FROM python:3.11
LABEL authors="mikad"

WORKDIR /
COPY app/ app/
RUN pip install --trusted-host pypi.python.org -r app/requirements.txt

CMD ["uvicorn", "app.api.main:app", "--host", "0.0.0.0", "--port", "8000"]