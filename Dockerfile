FROM node:10

WORKDIR /usr/src/app
ENV BALLOT_VERSION 1.0.1

# Keep the .git directory in order to properly report version
COPY ./package.json .

RUN apt-get update \
    && apt-get install -y jq python git build-essential --no-install-recommends \
    && npm install

COPY ./ ./

ADD https://github.com/scality/ballot/releases/download/v${BALLOT_VERSION}/ballot-v${BALLOT_VERSION}-linux-amd64 /usr/src/app/ballot
RUN chmod +x /usr/src/app/ballot

ENV NO_PROXY localhost,127.0.0.1
ENV no_proxy localhost,127.0.0.1
