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

RUN sed -i 's/load_examples = True/load_examples = False/' ./airflow.cfg

ARG install_dev
COPY --chown=airflow:airflow requirements.dev.txt ./
COPY --chown=airflow:airflow run_test.sh ./
COPY --chown=airflow:airflow dag_pipeline_test ./dag_pipeline_test
COPY --chown=airflow:airflow dags ./dags
COPY --chown=airflow:airflow data_pipeline ./data_pipeline

RUN if [ "${install_dev}" = "y" ]; then pip install --user -r requirements.dev.txt; fi
RUN if [ "${install_dev}" = "y" ]; then chmod +x run_test.sh; fi
#RUN if [ "${install_dev}" = "y" ]; then ./run_test.sh; fi

COPY --chown=airflow:airflow setup.py ./


#RUN pip install -e . --user --no-dependencies
ENTRYPOINT []


#ENTRYPOINT ["/bin/bash", "./run_test.sh"]
