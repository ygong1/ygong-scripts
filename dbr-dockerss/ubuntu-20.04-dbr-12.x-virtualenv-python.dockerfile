
WORKDIR /databricks


ENV LOW_PRIVILEGED_LIBRARY_INSTALLATION_USER=libraries
RUN useradd $LOW_PRIVILEGED_LIBRARY_INSTALLATION_USER \
    && usermod -L $LOW_PRIVILEGED_LIBRARY_INSTALLATION_USER


# Free some disk space from conda packages because we will setup Python with virtualenv.
RUN /databricks/conda/bin/conda clean --packages --tarballs

# Make conda command not accessible by default.
RUN unlink /etc/profile.d/conda.sh


# Install python 3.9 from ubuntu.
# Install pip via get-pip.py bootstrap script and install versions that match Anaconda distribution.
RUN apt-get --allow-releaseinfo-change-origin update \
    && apt-get install software-properties-common -y python3.9 python3.9-dev python3.9-distutils \
    && curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && /usr/bin/python3.9 get-pip.py pip==21.2.4 setuptools==58.0.4 wheel==0.37.0 \
    && rm get-pip.py


RUN /usr/local/bin/pip3.9 install --no-cache-dir virtualenv==20.8.0 \
    && sed -i -r 's/^(PERIODIC_UPDATE_ON_BY_DEFAULT) = True$/\1 = False/' /usr/local/lib/python3.9/dist-packages/virtualenv/seed/embed/base_embed.py \
    && /usr/local/bin/pip3.9 download pip==21.2.4 --dest \
    /usr/local/lib/python3.9/dist-packages/virtualenv_support/

