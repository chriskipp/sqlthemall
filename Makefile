# Some simple testing tasks (sorry, UNIX only).


SOURCEDIR = sqlthemall
TESTDIR = tests

VENV           = _virtualenv
VENV_PYTHON    = $(VENV)/bin/python
SYSTEM_PYTHON  = $(or $(shell which python3), $(shell which python))
# If virtualenv exists, use it. If not, find python using PATH
PYTHON         = $(or $(wildcard $(VENV_PYTHON)), $(SYSTEM_PYTHON))
AUTOFLAKE      = $(or $(VENV)/bin/autoflake, autoflake)
PYDOCSTRINGFORMATTER = $(or $(VENV)/bin/pydocstringformatter, pydocstringformatter)

## Dev/build environment

$(VENV_PYTHON):
	$(SYSTEM_PYTHON) -m venv $(VENV)

venv: $(VENV_PYTHON)

deps: venv
	$(PYTHON) -m pip install --upgrade pip setuptools wheel build twine

install: deps
	$(PYTHON) -m pip install .

install-dev: install
	# Dev dependencies
	$(PYTHON) -m pip install '.[testing,typing,linting]'

.PHONY: venv deps install install-dev

black: install-dev
	$(PYTHON) -m black --line-length 79 --safe $(SOURCEDIR)

autoflake: install-dev
	$(AUTOFLAKE) -v -v --in-place --recursive --remove-all-unused-imports --ignore-init-module-imports $(SOURCEDIR)

isort: install-dev
	$(PYTHON) -m isort $(SOURCEDIR)

mypy: install-dev
	$(PYTHON) -m mypy --install-types --non-interactive --ignore-missing-imports $(SOURCEDIR)

pdocstr: install-dev
	$(PYDOCSTRINGFORMATTER) --linewrap-full-docstring --write  --max-line-length 79 $(SOURCEDIR)

flake: install-dev
	$(PYTHON) -m flake8 --statistics --show-source --ignore S310,G004 --requirements-file requirements.txt $(SOURCEDIR)

pylint: install-dev
	$(PYTHON) -m pylint --rcfile .pylintrc $(SOURCEDIR)

autolint: black isort mypy pdocstr autoflake clean
lint: flake pylint

test: install-dev
	$(PYTHON) -m pytest -vvv tests

cov: install-dev
	$(PYTHON) -m pytest -vvv --cov=sqlthemall --cov-report=html:coverage tests

test-all: install-dev
	$(PYTHON) -m tox

#upload: test-all build-dist
#	_virtualenv/bin/twine upload dist/*
#	make clean

.PHONY: build-dist

build-dist: clean
	_virtualenv/bin/pyproject-build

clean:
	rm -rf `find . -name __pycache__`
	rm -rf `find . -type d -name '*.egg-info' `
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -f `find . -type f -name '@*' `
	rm -f `find . -type f -name '#*#' `
	rm -f `find . -type f -name '*.orig' `
	rm -f `find . -type f -name '*.rej' `
	rm -rf $(VENV)
	rm -rf .coverage
	rm -rf coverage
	rm -rf build
	rm -rf htmlcov
	rm -rf dist
	rm -rf .mypy_cache
	rm -rf .pytest_cache
	rm -rf .tox
	rm -f .pyre_configuration
	rm -rf .pyre
	rm -f test.sqlite

update_req: deps
	sed 's/[<>=].*/\\s/' requirements.txt requirements-dev.txt > _requirements.txt
	sh -c '. _virtualenv/bin/activate; ( $(PYTHON) -m pip list --outdated | grep -if _requirements.txt ) || echo "No outdated packages."'
	rm _requirements.txt

