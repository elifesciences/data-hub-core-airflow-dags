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
	# only dev compose file has "init" service defined
	@if [ "$(DOCKER_COMPOSE)" = "$(DOCKER_COMPOSE_DEV)" ]; then \
		$(DOCKER_COMPOSE) build init; \
	fi
	$(DOCKER_COMPOSE) build airflow-dev-base-image airflow-dev


test: build-dev
	$(DOCKER_COMPOSE) run --rm airflow-dev /bin/sh ./project-tests.sh


watch: build-dev
	$(DOCKER_COMPOSE) run --rm airflow-dev python -m pytest_watch


deploy-sciencebeam:
	$(DOCKER_COMPOSE) run --rm --no-deps --entrypoint='' airflow-webserver \
		python -m sciencebeam_airflow.tools.deploy_sciencebeam $(ARGS)


start:
	$(eval SERVICE_NAMES = $(shell docker-compose config --services | grep -v 'airflow-dev'))
	$(DOCKER_COMPOSE) up --build -d --scale airflow-worker=2 $(SERVICE_NAMES)


stop:
	$(DOCKER_COMPOSE) down


restart:
	$(DOCKER_COMPOSE) restart


logs:
	$(DOCKER_COMPOSE) logs -f


clean:
	$(DOCKER_COMPOSE) down -v


web-shell:
	$(DOCKER_COMPOSE) run --no-deps airflow-webserver bash


web-exec:
	$(DOCKER_COMPOSE) exec airflow-webserver /entrypoint.sh bash


worker-shell:
	$(DOCKER_COMPOSE) run --no-deps airflow-worker bash


worker-exec:
	$(DOCKER_COMPOSE) exec airflow-worker /entrypoint.sh bash


dags-list:
	$(DOCKER_COMPOSE) exec airflow-webserver /entrypoint.sh airflow list_dags


dags-unpause:
	$(eval DAG_IDS = $(shell \
		$(DOCKER_COMPOSE) exec airflow-webserver /entrypoint.sh airflow list_dags \
		| grep -P "^(sciencebeam_|grobid_)\S+" \
	))
	@echo DAG_IDS=$(DAG_IDS)
	@for DAG_ID in $(DAG_IDS); do \
		echo DAG_ID=$${DAG_ID}; \
		$(DOCKER_COMPOSE) exec airflow-webserver /entrypoint.sh airflow unpause $${DAG_ID}; \
	done



ci-build-and-test:
	make DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" build test


ci-clean:
	$(DOCKER_COMPOSE_CI) down -v


#run:
#    docker-compose run --rm dhub ./run_test.sh
#    docker-compose build  --build-arg install_dev=y
