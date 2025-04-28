# Some simple testing tasks (sorry, UNIX only).


PYTHON_VERSION = 3.10
PYTHON = python$(PYTHON_VERSION)
VENV_PATH = $(shell poetry env use --quiet $(PYTHON); poetry env info --path)

SOURCEDIR = sqlthemall
TESTDIR = tests

VENV           = _virtualenv
VENV_PYTHON    = $(VENV)/bin/python
SYSTEM_PYTHON  = $(or $(shell which python3), $(shell which python))
# If virtualenv exists, use it. If not, find python using PATH
PYTHON         = $(or $(wildcard $(VENV_PYTHON)), $(SYSTEM_PYTHON))
MYPY           = $(or $(VENV)/bin/mypy, mypy)
AUTOFLAKE      = $(or $(VENV)/bin/autoflake, autoflake)

## Dev/build environment

$(VENV_PYTHON):
	$(SYSTEM_PYTHON) -m venv $(VENV)

venv: $(VENV_PYTHON)

deps: venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install --upgrade setuptools
	$(PYTHON) -m pip install --upgrade wheel
	$(PYTHON) -m pip install --upgrade build twine
	$(PYTHON) -m pip install -r requirements.txt
	# Dev dependencies
	$(PYTHON) -m pip install -r requirements-dev.txt

.PHONY: venv deps


black: deps
	$(PYTHON) -m black --line-length 79 --safe $(SOURCEDIR)

autoflake: deps
	$(AUTOFLAKE) -v -v --in-place --recursive --remove-all-unused-imports --ignore-init-module-imports $(SOURCEDIR)

isort:
	$(PYTHON) -m isort $(SOURCEDIR)

mypy: deps
	$(MYPY) --install-types --non-interactive --ignore-missing-imports $(SOURCEDIR)

pdocstr:
	pydocstringformatter --linewrap-full-docstring --write  --max-line-length 79 $(SOURCEDIR)

flake: deps
	$(PYTHON) -m flake8 --statistics --show-source --ignore S310 --requirements-file requirements.txt $(SOURCEDIR)

pylint: deps
	$(PYTHON) -m pylint --rcfile .pylintrc $(SOURCEDIR)

autolint: black isort mypy pdocstr autoflake clean
lint: flake pylint

install: deps
	$(PYTHON) -m pip install .

test: deps
	$(PYTHON) -m pip install -r requirements-dev.txt
	$(PYTHON) -m pytest -vvv tests

cov: deps
	$(PYTHON) -m pip install -r requirements-dev.txt
	$(PYTHON) -m pytest -vvv --cov=sqlthemall --cov-report=html:coverage tests

test-all: deps
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
	rm -rf _requirements.txt
	rm -rf _requirements-dev.txt
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
	poetry env remove --all

bootstrap:
	poetry lock
	poetry install

update_req: deps
	sed 's/[<>=].*/\\s/' requirements.txt requirements-dev.txt > _requirements.txt
	sh -c '. _virtualenv/bin/activate; ( $(PYTHON) -m pip list --outdated | grep -if _requirements.txt ) || echo "No outdated packages."'
	rm _requirements.txt

