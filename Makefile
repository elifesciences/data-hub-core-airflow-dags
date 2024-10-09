#!/usr/bin/make -f

DOCKER_COMPOSE_DEV = docker-compose
DOCKER_COMPOSE_CI = docker-compose -f docker-compose.yml
DOCKER_COMPOSE = $(DOCKER_COMPOSE_DEV)


VENV = venv
PIP = $(VENV)/bin/pip
PYTHON = PYTHONPATH=dags $(VENV)/bin/python


PYTEST_WATCH_MODULES = tests/unit_test


.require-%:
	@ if [ "${${*}}" = "" ]; then \
		echo "Environment variable $* not set"; \
		exit 1; \
	fi


venv-clean:
	@if [ -d "$(VENV)" ]; then \
		rm -rf "$(VENV)"; \
	fi


venv-create:
	python3 -m venv $(VENV)


venv-activate:
	chmod +x venv/bin/activate
	bash -c "venv/bin/activate"


dev-install:
	$(PIP) install --disable-pip-version-check -r requirements.build.txt
	SLUGIFY_USES_TEXT_UNIDECODE=yes \
	$(PIP) install --disable-pip-version-check \
		-r requirements.monitoring.txt \
		-r requirements.txt \
		-r requirements.dev.txt
	$(PIP) install --disable-pip-version-check -e . --no-deps


dev-venv: venv-create dev-install


dev-flake8:
	$(PYTHON) -m flake8 data_pipeline dags tests


dev-pylint:
	$(PYTHON) -m pylint data_pipeline dags tests


dev-mypy:
	$(PYTHON) -m mypy data_pipeline dags tests


dev-lint: dev-flake8 dev-pylint dev-mypy


dev-unittest:
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS) tests/unit_test


dev-dagtest:
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS) tests/dag_validation_test


dev-integration-test: dev-install
	$(VENV)/bin/airflow upgradedb
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS) tests/integration_test


dev-watch:
	$(PYTHON) -m pytest_watch -- -p no:cacheprovider $(ARGS) $(PYTEST_WATCH_MODULES)


dev-test: dev-lint dev-unittest dev-dagtest


dev-run-elife-articles-xml:
	ELIFE_ARTICLE_XML_CONFIG_FILE_PATH=sample_data_config/elife-article-xml/elife-article-xml.config.yaml \
	$(PYTHON) -m data_pipeline.elife_article_xml.cli


dev-run-web-api:  .require-DATA_PIPELINE_ID
	WEB_API_CONFIG_FILE_PATH=sample_data_config/web-api/web-api-data-pipeline.config.yaml \
	$(PYTHON) -m data_pipeline.generic_web_api.cli \
		--data-pipeline-id=$(DATA_PIPELINE_ID) $(ARGS)


build:
	$(DOCKER_COMPOSE) build data-hub-dags


build-dev:
	$(DOCKER_COMPOSE) build data-hub-dags-dev


flake8:
	$(DOCKER_COMPOSE) run --rm data-hub-dags-dev \
		python -m flake8 data_pipeline dags tests

pylint:
	$(DOCKER_COMPOSE) run --rm data-hub-dags-dev \
		python -m pylint data_pipeline dags tests

mypy:
	$(DOCKER_COMPOSE) run --rm data-hub-dags-dev \
		python -m mypy data_pipeline dags tests

lint: flake8 pylint mypy

dagtest:
	$(DOCKER_COMPOSE) run --rm data-hub-dags-dev \
		python -m pytest -p no:cacheprovider $(ARGS) tests/dag_validation_test

unittest:
	$(DOCKER_COMPOSE) run --rm data-hub-dags-dev \
		python -m pytest -p no:cacheprovider $(ARGS) tests/unit_test

test: lint unittest

watch:
	$(DOCKER_COMPOSE) run --rm data-hub-dags-dev \
		python -m pytest_watch -- -p no:cacheprovider $(ARGS) $(PYTEST_WATCH_MODULES)

docker-show-ftpserver-logs-and-fail:
	$(DOCKER_COMPOSE) logs "test-ftpserver" && exit 1

docker-wait-for-ftpserver:
	$(DOCKER_COMPOSE) run --rm wait-for-it \
		"test-ftpserver:21" \
		--timeout=30 \
		--strict \
		-- echo "test-ftpserver is up" \
		|| $(MAKE) docker-show-ftpserver-logs-and-fail

test-ftpserver-start:
	$(DOCKER_COMPOSE) up -d test-ftpserver


airflow-start:
	$(DOCKER_COMPOSE) up worker webserver test-ftpserver

airflow-start-detach:
	$(DOCKER_COMPOSE) up --detach worker webserver test-ftpserver


airflow-stop:
	$(DOCKER_COMPOSE) down

k3s-start:
	$(DOCKER_COMPOSE) up k3s-server k3s-agent

k3s-start-detach :
	$(DOCKER_COMPOSE) up --detach k3s-server k3s-agent

opensearch-start:
	$(DOCKER_COMPOSE) up -d opensearch opensearch-dashboards


opensearch-stop:
	$(DOCKER_COMPOSE) stop opensearch opensearch-dashboards


opensearch-logs:
	$(DOCKER_COMPOSE) logs -f opensearch opensearch-dashboards


test-exclude-e2e: build-dev
	$(DOCKER_COMPOSE) run --rm data-hub-dags-dev ./run_test.sh


clean:
	$(DOCKER_COMPOSE) down -v

airflow-db-check-migrations:
	$(DOCKER_COMPOSE) run --rm  webserver db check-migrations

airflow-db-migrate:
	$(DOCKER_COMPOSE) run --rm  webserver db migrate

airflow-initdb:
	$(DOCKER_COMPOSE) run --rm  webserver db init


end2end-test:
	$(MAKE) clean
	$(MAKE) airflow-db-migrate
	$(MAKE) airflow-initdb
	$(MAKE) test-ftpserver-start
	$(MAKE) docker-wait-for-ftpserver
	$(DOCKER_COMPOSE) run --rm  test-client
	$(MAKE) clean


data-hub-pipelines-shell:
	$(DOCKER_COMPOSE) run --rm data-hub-pipelines \
	bash


data-hub-pipelines-run-elife-articles-xml:
	$(DOCKER_COMPOSE) run --rm data-hub-pipelines \
	python -m data_pipeline.elife_article_xml.cli


data-hub-pipelines-run-web-api:  .require-DATA_PIPELINE_ID
	$(DOCKER_COMPOSE) run --rm data-hub-pipelines \
		python -m data_pipeline.generic_web_api.cli \
		--data-pipeline-id=$(DATA_PIPELINE_ID) $(ARGS)


ci-test-exclude-e2e: build-dev
	$(MAKE) DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		test-exclude-e2e


ci-build-main-image:
	$(MAKE) DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		build


ci-build-and-end2end-test:
	$(MAKE) DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		build-dev \
		end2end-test

ci-end2end-test-clear-logs:
	$(DOCKER_COMPOSE_CI) exec -T worker bash -c \
		'rm -rf logs/*'

ci-end2end-test-logs:
	$(DOCKER_COMPOSE_CI) exec -T worker bash -c \
		'cat logs/*/*/*/*.log'

ci-clean:
	$(DOCKER_COMPOSE_CI) down -v


retag-push-image:
	docker tag  $(EXISTING_IMAGE_REPO):$(EXISTING_IMAGE_TAG) $(IMAGE_REPO):$(IMAGE_TAG)
	docker push  $(IMAGE_REPO):$(IMAGE_TAG)
