# Use separate builder to retrieve & build node modules
FROM node:16.15.1-bullseye-slim AS builder

WORKDIR /usr/src/app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        git \
        jq \
        python3 \
        ssh

COPY ./package.json .
RUN npm install

################################################################################
FROM node:16.15.1-slim

WORKDIR /usr/src/app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        jq \
        python3 \
        python3-pip \
        python3-setuptools \
    && \
    SUPERVISORURL="https://files.pythonhosted.org/packages/d3/7f/c780b7471ba0ff4548967a9f7a8b0bfce222c3a496c3dfad0164172222b0" && \
    SUPERVISORTARFILE="supervisor-4.2.2.tar.gz" && \
    pip3 install $SUPERVISORURL/$SUPERVISORTARFILE && \
    rm -rf /var/lib/apt/lists/* 

ENV BALLOT_VERSION 1.0.3
ADD https://github.com/scality/ballot/releases/download/v${BALLOT_VERSION}/ballot-v${BALLOT_VERSION}-linux-amd64 /usr/src/app/ballot
RUN chmod +x /usr/src/app/ballot

COPY ./ ./
COPY --from=builder /usr/src/app/node_modules ./node_modules/

ENV NO_PROXY localhost,127.0.0.1
ENV no_proxy localhost,127.0.0.1

## This section duplicates S3C Federation Dockerfile, this needs to be refactored
# Rename the "node" user to "scality" and setup associated env variables
# for augmented images to use.
RUN usermod --login scality --home /home/scality --move-home node && \
    groupmod --new-name scality node
ENV USER="scality"
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
