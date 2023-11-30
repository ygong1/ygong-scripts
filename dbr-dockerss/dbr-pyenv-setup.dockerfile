WORKDIR /databricks

ARG PYENV_VERSION=2.3.11

# Install pyenv {pyenv_version} and build tools required for compiling python.
# https://github.com/pyenv/pyenv/wiki#suggested-build-environment lists required build tools
# for Ubuntu, but some of them are already installed, so we only install missing ones.
# Note `llvm` is excluded on purpose to reduce the image size.
RUN apt-get --allow-releaseinfo-change-origin update -y \
        && apt-get install -y libffi-dev libncursesw5-dev libreadline-dev libsqlite3-dev libxmlsec1-dev tk-dev \
        && wget https://github.com/pyenv/pyenv/archive/refs/tags/v${PYENV_VERSION}.tar.gz -O pyenv.tar.gz \
        && mkdir -p /databricks/.pyenv \
        && tar -xvf pyenv.tar.gz --strip-components 1 -C /databricks/.pyenv \
        && rm pyenv.tar.gz

ENV PYENV_ROOT="/databricks/.pyenv"
ENV PATH="$PYENV_ROOT/bin:$PATH"
