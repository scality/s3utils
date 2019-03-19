FROM node:8-slim

WORKDIR /usr/src/app
ENV MONGO_VER 3.6.8

# Keep the .git directory in order to properly report version
COPY ./package.json .

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2930ADAE8CAF5059EE73BB4B58712A2291FA4AD5 \
    && echo "deb http://repo.mongodb.org/apt/debian jessie/mongodb-org/3.6 main" | tee /etc/apt/sources.list.d/mongodb-org.list \
    && apt-get update \
    && apt-get install -y jq python git build-essential vim mongodb-org-shell=$MONGO_VER mongodb-org-tools=$MONGO_VER --no-install-recommends \
    && npm install

COPY ./ ./

ENV NO_PROXY localhost,127.0.0.1
ENV no_proxy localhost,127.0.0.1

