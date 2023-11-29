SHELL = /bin/bash

PY = ./.venv/bin/python -m

src = .

isort = $(PY) isort $(src)
black = $(PY) black $(src)

.PHONY: install ## install requirements in virtual env 
install:
	rm -rf .venv
	python3.10 -m venv .venv && \
		${PY} pip install --upgrade pip && \
		${PY} pip install -r docker/requirements.txt \
			black \
			isort \
			kubernetes \
			--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.10.txt"

.PHONY: format  ## Auto-format the source code (isort, black)
format:
	$(isort)
	$(black)
