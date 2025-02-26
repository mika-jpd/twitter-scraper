FROM python:3.11
LABEL authors="mikad"

WORKDIR /
COPY app/ app/

RUN pip install --trusted-host pypi.python.org -r app/requirements.txt

CMD ["rqscheduler", "--host", "redis", "--port", "6379"]