# Makefile

PYTHON = python3
SOURCEDIR = sqlthemall
TESTDIR = tests
CONTAINERNAME =
TESTCONTAINER =


black: bootstrap
	sh -c '. _virtualenv/bin/activate; black --line-length 79 --safe $(SOURCEDIR) $(TESTDIR)'

autoflake: bootstrap
	sh -c '. _virtualenv/bin/activate; autoflake -v -v --in-place --recursive --remove-all-unused-imports --ignore-init-module-imports $(SOURCEDIR) $(TESTDIR)'

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

mypy:
	sh -c '. _virtualenv/bin/activate; mypy --install-types --non-interactive --ignore-missing-imports --exclude setup.py $(SOURCEDIR) $(TESTDIR)'

check: bootstrap mypy bandit clean


test: bootstrap
	sh -c '. _virtualenv/bin/activate; pytest -vvv --disable-warnings $(TESTDIR)'

test_with_warnings: bootstrap
	sh -c '. _virtualenv/bin/activate; pytest -vvv $(TESTDIR)'

coverage: bootstrap
	sh -c '. _virtualenv/bin/activate; pytest -vvv --cov=$(SOURCEDIR) --disable-warnings --cov-report=html:coverage $(TESTDIR)'

tox:
	tox

build-dist:: bootstrap
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
	rm -f .pyre_configuration
	rm -rf .pyre
	rm -rf uploads

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

