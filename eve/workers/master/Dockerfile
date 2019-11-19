FROM ubuntu:trusty

#
# Install apt packages needed by the buildchain
#
ENV LANG C.UTF-8
COPY buildbot_worker_packages.list s3utils_packages.list /tmp/
RUN apt-get update -q && apt-get -qy install curl apt-transport-https \
    && apt-get install -qy software-properties-common python-software-properties \
    && add-apt-repository ppa:ubuntu-toolchain-r/test \
    && apt-get update -q \
    && cat /tmp/buildbot_worker_packages.list | xargs apt-get install -qy \
    && cat /tmp/s3utils_packages.list | xargs apt-get install -qy \
    && git clone https://github.com/tj/n.git \
    && make -C ./n \
    && n 10 latest \
    && curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | sudo apt-key add - \
    && echo "deb https://dl.yarnpkg.com/debian/ stable main" | sudo tee /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && apt-get install yarn \
    && pip install pip==9.0.1 \
    && rm -rf /var/lib/apt/lists/* \
    && rm -f /tmp/*_packages.list

#
# Install usefull nodejs dependencies
#

RUN yarn global add jest

#
# Add user eve
#

RUN adduser -u 1042 --home /home/eve --disabled-password --gecos "" eve \
    && adduser eve sudo \
    && sed -ri 's/(%sudo.*)ALL$/\1NOPASSWD:ALL/' /etc/sudoers

#
# Run buildbot-worker on startup
#

ARG BUILDBOT_VERSION=0.9.12
RUN pip install yamllint
RUN pip install buildbot-worker==$BUILDBOT_VERSION

USER eve
ENV HOME /home/eve
#
# Setup nodejs environmnent
#

ENV CXX=g++-4.9
ENV LANG C.UTF-8


WORKDIR /home/eve/workspace
CMD buildbot-worker create-worker . "$BUILDMASTER:$BUILDMASTER_PORT" "$WORKERNAME" "$WORKERPASS" \
    && buildbot-worker start --nodaemon
