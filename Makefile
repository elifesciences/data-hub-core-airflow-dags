#!/usr/bin/make -f

DOCKER_COMPOSE_DEV = docker-compose
DOCKER_COMPOSE_CI = docker-compose -f docker-compose.yml
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
	$(PIP) install -e .

	export SLUGIFY_USES_TEXT_UNIDECODE=yes

	$(PIP) install -r requirements.txt

	$(PIP) install -r requirements.dev.txt


dev-venv: venv-create dev-install


dev-flake8:
	$(PYTHON) -m flake8 data_pipeline dags tests


dev-pylint:
	$(PYTHON) -m pylint data_pipeline dags tests


dev-lint: dev-flake8 dev-pylint

dev-unittest:
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS) tests/unit_test

dev-integration-test:
	pip install -e .  --no-dependencies
	airflow upgradedb
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS) tests/integration_test

dev-test: dev-lint dev-unittest dev-integration-test


build:
	$(DOCKER_COMPOSE) build datahub_image


build-dev:
	$(DOCKER_COMPOSE) build datahub-dags-dev


test: build-dev
	$(DOCKER_COMPOSE) run --rm datahub-dags-dev ./run_test.sh


ci-clean:
	$(DOCKER_COMPOSE_CI) down -v
