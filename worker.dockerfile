FROM python:3.11
LABEL authors="mikad"

# Install dumb-init
RUN apt-get update && apt-get install -y dumb-init

WORKDIR /
COPY app/ app/

RUN pip install --trusted-host pypi.python.org -r app/requirements.txt

# For macOS fork() issue
ENV OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

RUN apt-get update &&  \
    apt-get install -y wget unzip && \
    wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb &&  \
    apt install -y ./google-chrome-stable_current_amd64.deb && \
    #dpkg -i google-chrome-stable_current_arm64.deb && \
    #apt --fix-broken install && \
    rm google-chrome-stable_current_amd64.deb &&  \
    apt-get clean

# Use dumb-init as the entrypoint
ENTRYPOINT ["dumb-init", "--"]
CMD ["python", "-m", "app.worker.worker"]