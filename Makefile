#!/usr/bin/make -f

DOCKER_COMPOSE_CI = docker-compose
DOCKER_COMPOSE_DEV = docker-compose -f docker-compose.yml -f docker-compose.dev.override.yml
DOCKER_COMPOSE = $(DOCKER_COMPOSE_CI)


VENV = venv
PIP = $(VENV)/bin/pip
PYTHON = PYTHONPATH=dags $(VENV)/bin/python


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
	SLUGIFY_USES_TEXT_UNIDECODE=yes $(PIP) install -r requirements.txt
	$(PIP) install -r requirements.dev.txt
	$(PIP) install -e . --no-deps


dev-venv: venv-create dev-install


dev-flake8:
	$(PYTHON) -m flake8 data_pipeline dags tests


dev-pylint:
	$(PYTHON) -m pylint data_pipeline dags tests


dev-lint: dev-flake8 dev-pylint


dev-unittest:
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS) tests/unit_test


dev-dagtest:
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS) tests/dag_validation_test


dev-integration-test: dev-install
	(VENV)/bin/airflow upgradedb
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS) tests/integration_test


dev-test: dev-lint dev-unittest dev-dagtest


build:
	$(DOCKER_COMPOSE) build data-hub_image


build-dev:
	$(DOCKER_COMPOSE) build data-hub-dags-dev


ci-test-exclude-e2e: build-dev
	$(DOCKER_COMPOSE) run --rm data-hub-dags-dev ./run_test.sh


ci-end2end-test: build-dev
	$(DOCKER_COMPOSE) run --rm  test-client
	make ci-clean


dev-env: build-dev
	$(DOCKER_COMPOSE_DEV) up  --scale dask-worker=1 scheduler


dev-test-exclude-e2e: build-dev
	$(DOCKER_COMPOSE_DEV) run --rm data-hub-dags-dev ./run_test.sh


dev-end2end-test: build-dev
	$(DOCKER_COMPOSE_DEV) run --rm  test-client
	make ci-clean


ci-clean:
	$(DOCKER_COMPOSE) down -v
