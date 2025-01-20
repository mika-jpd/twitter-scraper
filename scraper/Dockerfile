FROM python:3.10
LABEL authors="mikad"

WORKDIR /app
ADD . .
RUN pip install --trusted-host pypi.python.org -r requirements.txt

RUN apt-get update &&  \
    apt-get install -y wget unzip && \
    wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb &&  \
    apt install -y ./google-chrome-stable_current_amd64.deb && \
    #dpkg -i google-chrome-stable_current_arm64.deb && \
    #apt --fix-broken install && \
    rm google-chrome-stable_current_amd64.deb &&  \
    apt-get clean

CMD ["python", "redis_worker.py"]