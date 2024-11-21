SHELL = /bin/bash
.DEFAULT_GOAL = install

PY = ./.venv/bin/python -m

PY_SNOWBIRD = ./infrastructure/.snowbird-venv/bin/python -m

src = .

isort = $(PY) isort $(src)
black = $(PY) black $(src)

.PHONY: install ## install requirements in virtual env
install: _install_airflow _install_snowbird

_install_airflow:
	rm -rf .venv
	python3.10 -m venv .venv && \
		${PY} pip install --upgrade pip && \
		${PY} pip install -r docker/airflow/requirements.txt \
			black \
			isort \
			kubernetes \
			--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.10.txt"

_install_snowbird:
	rm -rf infrastructure/.snowbird-venv
	python3.10 -m venv infrastructure/.snowbird-venv && \
		${PY_SNOWBIRD} pip install --upgrade pip && \
		${PY_SNOWBIRD} pip install snowbird@git+https://github.com/navikt/snowbird@v0.1

.PHONY: format  ## Auto-format the source code (isort, black)
format:
	$(isort)
	$(black)
