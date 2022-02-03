FROM node:16

WORKDIR /usr/src/app
ENV BALLOT_VERSION 1.0.3

# Keep the .git directory in order to properly report version
COPY ./package.json .

RUN apt-get update \
    && apt-get install -y jq python python-setuptools git build-essential --no-install-recommends \
    && npm install && \
    SUPERVISORURL="https://files.pythonhosted.org/packages/d3/7f/c780b7471ba0ff4548967a9f7a8b0bfce222c3a496c3dfad0164172222b0/supervisor-4.2.2.tar.gz" && \
    SUPERVISORTARFILE="supervisor-4.2.2.tar.gz" && \
    wget $SUPERVISORURL && \
    easy_install ./$SUPERVISORTARFILE && \
    rm -v ./$SUPERVISORTARFILE

COPY ./ ./

ADD https://github.com/scality/ballot/releases/download/v${BALLOT_VERSION}/ballot-v${BALLOT_VERSION}-linux-amd64 /usr/src/app/ballot
RUN chmod +x /usr/src/app/ballot

ENV NO_PROXY localhost,127.0.0.1
ENV no_proxy localhost,127.0.0.1

## This section duplicates S3C Federation Dockerfile, this needs to be refactored
# Create the "scality" user with associated env variables
# for augmented images to use.
ENV USER="scality"
RUN useradd -ms /bin/bash scality
ENV HOME_DIR="/home/${USER}"

# Create common Directories and matching env variables
# for augmented images to use
ENV LOG_DIR="/logs" CONF_DIR="/conf" DATA_DIR="/data" \
    SUP_RUN_DIR="/var/run/supervisor"
RUN \
    mkdir ${LOG_DIR} && \
    chown ${USER} ${LOG_DIR} &&\
    mkdir ${CONF_DIR} && \
    chown ${USER} ${CONF_DIR} &&\
    mkdir ${DATA_DIR} && \
    chown ${USER} ${DATA_DIR} &&\
    mkdir ${SUP_RUN_DIR} && \
    chown ${USER} ${SUP_RUN_DIR} &&\
    chmod 777 ${SUP_RUN_DIR}
