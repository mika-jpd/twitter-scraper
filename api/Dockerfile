FROM python:3.10
LABEL authors="mikad"

WORKDIR /app
ADD . .
RUN pip install --trusted-host pypi.python.org -r requirements.txt

CMD ["uvicorn", "launcher:app", "--host", "0.0.0.0", "--port", "8000"]