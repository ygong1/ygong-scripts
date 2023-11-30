
WORKDIR /databricks


# Create /databricks/python3 environment.
# We install pip and wheel so their executables show up under /databricks/python3/bin.
# We use `--system-site-packages` so python will fallback to system site packages.
# As a result, we do not need to install setuptools.
# We use `--no-download` so virtualenv will install the bundled pip and wheel.
RUN virtualenv --python=/usr/bin/python3.9 /databricks/python3 --system-site-packages --no-setuptools --no-download

# Now install all of libraries.
RUN export NPY_NUM_BUILD_JOBS=$(grep -c ^processor /proc/cpuinfo); \
  (/databricks/python3/bin/python -m pip install --no-cache-dir --requirement /databricks/.virtualenv-def/standard-requirements.txt; wait)


# Save the package dependency tree of all python packages in the environment
RUN /databricks/python3/bin/python -m pip install pipdeptree \
    && /databricks/python3/bin/python -m pipdeptree --json-tree > /databricks/.virtualenv-environments/py-package-deps.txt \
    && /databricks/python3/bin/python -m pip uninstall -y pipdeptree

# Pre-build the font cache for matplotlib, so users will not see the warning of
# "Matplotlib is building the font cache using fc-list. This may take a moment.".
RUN /databricks/python3/bin/python3 -c "from matplotlib.font_manager import FontManager"

# Use pip cache purge to cleanup the cache safely
RUN /databricks/python3/bin/pip cache purge

# Export the environment here so that the custom pip-installed packages are captured.
RUN /databricks/python3/bin/pip list --format=freeze | cut -d. -f1-3 > /databricks/.virtualenv-environments/environment.txt

