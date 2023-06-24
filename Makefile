# Makefile

PYTHON = python3
SOURCEDIR = sqlthemall


black:
	black --line-length 79 --safe $(SOURCEDIR)

autoflake:
	autoflake -v -v --in-place --recursive --remove-all-unused-imports --ignore-init-module-imports $(SOURCEDIR)

isort:
	isort $(SOURCEDIR)

mypy:
	mypy --install-types --non-interactive --ignore-missing-imports --exclude setup.py $(SOURCEDIR)

pdocstr:
	pydocstringformatter --linewrap-full-docstring --write  --max-line-length 79 $(SOURCEDIR)

flake:
	flake8 --statistics --show-source --ignore S310 --requirements-file requirements.txt $(SOURCEDIR)

pylint:
	pylint --rcfile .pylintrc $(SOURCEDIR)

autolint: black isort mypy pdocstr autoflake clean
lint: flake pylint

install:
ifeq ($(shell whoami),root)
		$(PYTHON) setup.py install
else
		$(PYTHON) setup.py install --user
endif

test: bootstrap
	sh -c '. _virtualenv/bin/activate; $(PYTHON) -m pip install -r requirements-dev.txt'
	sh -c '. _virtualenv/bin/activate; pytest -vvv tests'

cov: bootstrap
	sh -c '. _virtualenv/bin/activate; $(PYTHON) -m pip install -r requirements-dev.txt'
	sh -c '. _virtualenv/bin/activate; pytest -vvv --cov=$(SOURCEDIR) --disable-warnings --cov-report=html:coverage tests'

test-all: _virtualenv
	tox

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
	rm -rf _virtualenv
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
	rm -f test.sqlite

.PHONY: bootstrap

bootstrap: _virtualenv
	sh -c '. _virtualenv/bin/activate; $(PYTHON) setup.py install'

_virtualenv:
	python3 -m venv _virtualenv
	_virtualenv/bin/pip install --upgrade pip
	_virtualenv/bin/pip install --upgrade setuptools
	_virtualenv/bin/pip install --upgrade wheel
	_virtualenv/bin/pip install --upgrade build twine

update_req: bootstrap
	sed 's/[<>=].*/\\s/' requirements.txt requirements-dev.txt > _requirements.txt
	sh -c '. _virtualenv/bin/activate; ( $(PYTHON) -m pip list --outdated | grep -if _requirements.txt ) || echo "No outdated packages."'
	rm _requirements.txt

