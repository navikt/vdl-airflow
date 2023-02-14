SHELL = /bin/bash

PY = ./.venv/bin/python -m

src = .

isort = $(PY) isort $(src)
black = $(PY) black $(src)

.PHONY: install ## install requirements in virtual env 
install:
	python3.11 -m venv .venv && \
		${PY} pip install --upgrade pip && \
		${PY} pip install -r requirements.txt

.PHONY: format  ## Auto-format the source code (isort, black)
format:
	$(isort)
	$(black)
