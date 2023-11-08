# Makefile for python projects

PYTHON    = python3
SOURCEDIR = sqlthemall
TESTDIR   = tests

CONTAINERNAME =
TESTCONTAINER =



black:
	black --line-length 79 --skip  --safe $(SOURCEDIR) $(TESTDIR)

autoflake:
	autoflake -v -v --in-place --recursive --remove-all-unused-imports --ignore-init-module-imports $(SOURCEDIR) $(TESTDIR)

isort: bootstrap
	sh -c '. _virtualenv/bin/activate; isort $(SOURCEDIR)'

pdocstr: bootstrap
	sh -c '. _virtualenv/bin/activate; pydocstringformatter --linewrap-full-docstring --write  --max-line-length 79 $(SOURCEDIR)'

flake: bootstrap
	sh -c '. _virtualenv/bin/activate; flake8 --statistics --show-source --requirements-file requirements.txt $(SOURCEDIR) $(TESTDIR)'

pylint: bootstrap
	sh -c '. _virtualenv/bin/activate; pylint --rcfile .pylintrc $(SOURCEDIR) $(TESTDIR)'

pretty: bootstrap black autoflake isort pdocstr pylint clean

bandit:
	sh -c '. _virtualenv/bin/activate; bandit -r $(SOURCEDIR)'

pdocstr: bootstrap
	sh -c '. _virtualenv/bin/pydocstringformatter --linewrap-full-docstring --write  --max-line-length 79 $(SOURCEDIR)'

flake:
	flake8 --statistics --show-source --ignore S310 --requirements-file requirements.txt $(SOURCEDIR) $(TESTDIR)

pylint: bootstrap
	pylint --rcfile .pylintrc $(SOURCEDIR) $(TESTDIR)

pretty: bootstrap black autoflake isort pdocstr pylint clean


bandit:
	bandit -r $(SOURCEDIR) $(TESTDIR)

mypy:
	mypy --install-types --non-interactive --ignore-missing-imports --exclude setup.py $(SOURCEDIR) $(TESTDIR)

check: bootstrap mypy bandit clean



test: bootstrap
	sh -c '. _virtualenv/bin/activate; pytest -vvv $(TESTDIR)'

coverage: bootstrap
	sh -c '. _virtualenv/bin/activate; pytest -vvv --cov=$(SOURCEDIR) --disable-warnings --cov-report=html:coverage $(TESTDIR)'

tox:
	tox


install:
ifeq ($(shell whoami),root)
		$(PYTHON) setup.py install
else
		$(PYTHON) setup.py install --user
endif

build-dist: bootstrap
	curl -sSL https://install.python-poetry.org | _virtualenv/bin/$(PYTHON) -
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

_virtualenv:
	$(PYTHON) -m venv _virtualenv
	_virtualenv/bin/pip install --upgrade pip
	_virtualenv/bin/pip install --upgrade setuptools wheel build twine

bootstrap: _virtualenv
	_virtualenv/bin/pip install -r requirements.txt
	_virtualenv/bin/pip install -r requirements-dev.txt
	sh -c '. _virtualenv/bin/activate; $(PYTHON) setup.py install'


update_req: bootstrap
	_virtualenv/bin/pip list --outdated | sed '1,2d' up | awk '{ print "/"$1"[<>=][<>=]"$2"/s/"$2"/"$3"/" }' | sed -if - requirements.txt requirements-dev.txt


