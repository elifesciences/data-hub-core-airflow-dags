FROM puckel/docker-airflow:1.10.4
ARG install_dev

USER root

RUN apt update && apt install sudo -yqq
RUN usermod -aG sudo airflow
RUN echo "airflow ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers


RUN sed -i 's/LocalExecutor/SequentialExecutor/' /entrypoint.sh
COPY requirements.txt ./
RUN pip install --upgrade -r requirements.txt
RUN if [ "${install_dev}" = "y" ]; then  pip install bokeh; fi

USER airflow
COPY --chown=airflow:airflow requirements.dev.txt ./
RUN if [ "${install_dev}" = "y" ]; then pip install --user -r requirements.dev.txt; fi

ENV PATH /usr/local/airflow/.local/bin:$PATH

COPY --chown=airflow:airflow data_pipeline ./data_pipeline
COPY --chown=airflow:airflow dags ./dags
COPY --chown=airflow:airflow setup.py ./setup.py
RUN pip install -e . --user --no-dependencies

RUN mkdir /usr/local/airflow/auxiliary_data_pipeline_files
ENV AIRFLOW_APPLICATIONS_DIRECTORY_PATH /usr/local/airflow/auxiliary_data_pipeline_files
COPY app_aux_files ./auxiliary_data_pipeline_files/app_aux_files

COPY .pylintrc ./.pylintrc
COPY --chown=airflow:airflow tests ./tests
COPY --chown=airflow:airflow run_test.sh ./
RUN if [ "${install_dev}" = "y" ]; then chmod +x run_test.sh; fi

COPY --chown=airflow:airflow worker.sh ./
RUN chmod +x worker.sh

RUN mkdir -p $AIRFLOW_HOME/serve
RUN ln -s $AIRFLOW_HOME/logs $AIRFLOW_HOME/serve/log

ENTRYPOINT []
