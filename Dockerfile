FROM node:8-slim

WORKDIR /usr/src/app

# Keep the .git directory in order to properly report version
COPY ./package.json .

RUN apt-get update \
    && apt-get install -y jq python git build-essential --no-install-recommends \
    && npm install

COPY ./ ./

ENV NO_PROXY localhost,127.0.0.1
ENV no_proxy localhost,127.0.0.1
