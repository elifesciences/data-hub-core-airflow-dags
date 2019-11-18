FROM puckel/docker-airflow:1.10.4
ARG install_dev


USER root
RUN apt-get update -yqq \
    && pip install dask distributed
RUN if [ "${install_dev}" = "y" ]; then  pip install bokeh; fi
RUN sed -i 's/LocalExecutor/SequentialExecutor/' /entrypoint.sh

USER airflow

ENV PATH /usr/local/airflow/.local/bin:$PATH

COPY --chown=airflow:airflow data_pipeline ./data_pipeline
COPY --chown=airflow:airflow dags ./dags
COPY --chown=airflow:airflow tests ./tests
COPY --chown=airflow:airflow setup.py ./setup.py
COPY --chown=airflow:airflow requirements.txt ./
COPY --chown=airflow:airflow requirements.dev.txt ./
COPY --chown=airflow:airflow run_test.sh ./

RUN pip install --upgrade --user -r requirements.txt
RUN if [ "${install_dev}" = "y" ]; then pip install --user -r requirements.dev.txt; fi
RUN pip install -e . --user --no-dependencies


RUN if [ "${install_dev}" = "y" ]; then chmod +x run_test.sh; fi

ENTRYPOINT []
