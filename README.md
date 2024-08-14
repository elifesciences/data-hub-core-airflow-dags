# Data Hub Core Dags

This repository consists of the core main data pipelines that is deployed and run in the data hub's airflow instance.
Typically, a generic data pipeline is implemented here which needs to be appropriately configured for it to move the data from a data source, transform it, and write it to our data warehouse.
Generic data pipelines implemented so far include

* Crossref Event Data Pipeline
* Google Spreadsheet Data Pipeline
* S3 CSV Data Pipeline
* Generic Web Api Data Pipeline
* Google Analytics Data Pipeline
* Twitter Data Pipeline
* Gmail Data Pipeline

The sample configuration for the different data pipelines can be found in `sample_data_config` directory of this project

## Running The Pipeline Locally

This repo is designed to run in as a containerized application in the development environment.
To run this locally, review the `docker-compose.dev.override.yml` and `docker-compose.yaml` files, and ensure that the different credentials files, tokens or strings required by different data pipelines are correctly provided if you will have to run the data pipeline.
Following are the credentials that you may need to provide

* GCP's service account json key (mandatory for all data pipelines)
* AWS credentials
* Twitter's developer credentials
* CiviCRM's credentials
* Toggl credentials
* Gmail refresh token credentials

To run the application locally:

    make build-dev airflow-initdb airflow-start

To run the whole test on the application:

    make end2end-test

To run tests excluding the end to end tests:

    make test-exclude-e2e

To set up the development environment (virtual environment):

    # initial setup
    make dev-venv
    # update dependencies
    make dev-install

### Run k3s locally
  The `kubeconfig.yaml` file will be created automatically on localhost when we run `k3s-agent` and `k3s-server`.

    make k3s-start

  We need to update the server IP in the `kubeconfig.yaml` file to connect with Airflow. To obtain the relevant IP address, use the commands provided below. Once you have the IP, replace the `server` entry in the file with `server: https://<YOUR_IP>:6443`.

  To do this:

    # get network ids
    $ docker network ls
    NETWORK ID     NAME                                 DRIVER    SCOPE
    6840f3bbe3f1   data-hub-core-airflow-dags_default   bridge    local
    ...

    $ docker network inspect 6840f3bbe3f1
    ...
    # search for `data-hub-core-airflow-dags-k3s-server-1`
    {
      "Name": "data-hub-core-airflow-dags-k3s-server-1",
      "EndpointID": "",
      "MacAddress": "",
      "IPv4Address": "<YOUR_IP>",
      "IPv6Address": ""
    }

  We can also use kubectl locally by specifying the configuration file. To do this, we need to use `kubeconfig.yaml` with the localhost settings. Simply obtain a copy of the configuration with the localhost IP (`server: https://127.0.0.1:6443`) and save it as `kubeconfig_localhost.yaml`.

    $ kubectl --kubeconfig=k3s-config/kubeconfig_localhost.yaml get nodes
    ...

    $ kubectl --kubeconfig=k3s-config/kubeconfig_localhost.yaml get pods -A
    ...


## Project Folder/Package Organisation

* `dags` package consists of the airflow dags running the different data pipeline. Whenever the dag files ends with `controller.py`, the data pipeline in the file will typically trigger another data pipeline with some configuration values.
* `data_pipeline` package consist of the packages and libraries and functions needed to run the pipeline in `dags` package. Typically, there are
  * packages specific to each data pipeline dags under the `data_pipeline` package, and
  * a package `util` where functionalities shared by the different data pipelines packages are contained
* `tests` contains the tests run on this implementation. These include these types
  * unit tests
  * end to end tests
  * dag validation tests
* `sample_data_config` folder contains
  * the sample configurations for diffferent data pipelines
  * the bigquery data schema for the resulting transformed data from the data pipeline
  * the format of the `statefile` that is used to maintain the state of different pipeline if the need maintaining
  
## CI/CD

 This runs on Jenkins and follows the standard approaches used by the `eLife Data Team` for CI/CD.
 Note that as part of the CI/CD, another Jenkins pipeline is always triggered whenever there is a commit to the develop branch. The latest commit reference to a `develop` branch is passed on as a parameter to this Jenkins pipeline to be triggered, and this is used to update the [repo-list.json file](https://github.com/elifesciences/data-hub-airflow-image/blob/develop/repo-list.json) in another repository
