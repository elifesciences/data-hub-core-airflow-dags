FROM apache/airflow:2.3.0-python3.7

USER root

RUN sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 467B942D3A79BD29

RUN apt-get update \
  && apt-get install sudo gcc -yqq \
  && rm -rf /var/lib/apt/lists/*

RUN usermod -aG sudo airflow
RUN echo "airflow ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

USER airflow
COPY requirements.build.txt ./
RUN pip install --disable-pip-version-check -r requirements.build.txt

COPY requirements.monitoring.txt ./
RUN pip install --disable-pip-version-check -r requirements.monitoring.txt

# Note: install requirements together with previous requirements file to make conflicts visible
COPY requirements.txt ./
RUN pip install --disable-pip-version-check \
  -r requirements.monitoring.txt \
  -r requirements.txt

ARG install_dev=n
COPY --chown=airflow:airflow requirements.dev.txt ./
RUN if [ "${install_dev}" = "y" ]; then \
    pip install --user --disable-pip-version-check \
      -r requirements.monitoring.txt \
      -r requirements.txt \
      -r requirements.dev.txt; \
  fi

ENV PATH /home/airflow/.local/bin:$PATH

COPY --chown=airflow:airflow data_pipeline ./data_pipeline
COPY --chown=airflow:airflow dags ./dags
COPY --chown=airflow:airflow setup.py ./setup.py
RUN pip install -e . --user --no-dependencies

COPY --chown=airflow:airflow .flake8 .pylintrc run_test.sh ./
COPY --chown=airflow:airflow tests ./tests
RUN if [ "${install_dev}" = "y" ]; then chmod +x run_test.sh; fi

RUN mkdir -p $AIRFLOW_HOME/serve
RUN ln -s $AIRFLOW_HOME/logs $AIRFLOW_HOME/serve/log

ENTRYPOINT []
