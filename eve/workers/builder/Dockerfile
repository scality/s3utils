FROM ubuntu:18.04

# Install package needed for bootstraping the image as well as runtime
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    git \
    gnupg2 \
    locales \
    lsb-release \
    python3-buildbot-worker \
 && rm -rf /var/lib/apt/lists/*

# Set the locale
RUN sed -i -e 's/# en_GB.UTF-8 UTF-8/en_GB.UTF-8 UTF-8/' /etc/locale.gen && \
    dpkg-reconfigure --frontend=noninteractive locales && \
    update-locale LANG=en_GB.UTF-8

ENV LANG en_GB.UTF-8
ENV LC_ALL en_GB.UTF-8

# Install docker
ARG DOCKER_VERSION=18.06.1~ce~3-0~ubuntu
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - \
 && echo "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
    | tee -a /etc/apt/sources.list.d/docker-ce.list \
 && apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    docker-ce=${DOCKER_VERSION} \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /workdir

CMD buildbot-worker create-worker . ${BUILDMASTER}:${BUILDMASTER_PORT} ${WORKERNAME} ${WORKERPASS} \
      && buildbot-worker start --nodaemon