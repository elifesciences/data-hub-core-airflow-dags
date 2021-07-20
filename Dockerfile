FROM apache/airflow:1.10.15-python3.7
ARG install_dev=n

USER root

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

COPY requirements.txt ./
RUN pip install --disable-pip-version-check -r requirements.txt

COPY --chown=airflow:airflow requirements.dev.txt ./
RUN if [ "${install_dev}" = "y" ]; then pip install --user --disable-pip-version-check -r requirements.dev.txt; fi

ENV PATH /home/airflow/.local/bin:$PATH

COPY --chown=airflow:airflow data_pipeline ./data_pipeline
COPY --chown=airflow:airflow dags ./dags
COPY --chown=airflow:airflow monitoring ./monitoring
COPY --chown=airflow:airflow setup.py ./setup.py
RUN pip install -e . --user --no-dependencies

COPY --chown=airflow:airflow .flake8 .pylintrc run_test.sh ./
COPY --chown=airflow:airflow tests ./tests
RUN if [ "${install_dev}" = "y" ]; then chmod +x run_test.sh; fi

COPY --chown=airflow:airflow worker.sh ./
RUN chmod +x worker.sh

RUN mkdir -p $AIRFLOW_HOME/serve
RUN ln -s $AIRFLOW_HOME/logs $AIRFLOW_HOME/serve/log

ENTRYPOINT []
