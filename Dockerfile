FROM puckel/docker-airflow:1.10.4

USER root
RUN apt-get update -yqq \
    && pip install dask distributed \
    && pip install 'apache-airflow[google_auth]'

RUN sed -i 's/LocalExecutor/SequentialExecutor/' /entrypoint.sh


USER airflow
ENV EXECUTOR Sequential

ENV PATH /usr/local/airflow/.local/bin:$PATH

COPY --chown=airflow:airflow requirements.txt ./
RUN pip install --user -r requirements.txt

ARG install_dev
COPY --chown=airflow:airflow requirements.dev.txt ./
RUN if [ "${install_dev}" = "y" ]; then pip install --user -r requirements.dev.txt; fi

COPY --chown=airflow:airflow data_pipeline ./data_pipeline
COPY --chown=airflow:airflow setup.py ./


COPY --chown=airflow:airflow dags ./dags
RUN pip install -e . --user --no-dependencies