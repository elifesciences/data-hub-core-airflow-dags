#!/usr/bin/make -f

DOCKER_COMPOSE_DEV = docker-compose
DOCKER_COMPOSE = $(DOCKER_COMPOSE_DEV)

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


dev-integration-test:
	pip install -e .  --no-dependencies
	airflow upgradedb
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS) tests/integration_test

dev-test: dev-lint dev-unittest dev-dagtest


build:
	$(DOCKER_COMPOSE) build datahub_image


build-dev:
	$(DOCKER_COMPOSE) build datahub-dags-dev


ci-test-exclude-e2e: build-dev
	$(DOCKER_COMPOSE) run --rm datahub-dags-dev ./run_test.sh


ci-end2end-test: build-dev
	$(DOCKER_COMPOSE) run --rm  ci-test-client ./run_test.sh with-end-to-end

ci-env: build-dev
	$(DOCKER_COMPOSE) up  scheduler

ci-clean:
	$(DOCKER_COMPOSE) down -v
