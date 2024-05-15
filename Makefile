# Some simple testing tasks (sorry, UNIX only).


PYTHON_VERSION = 3.10
PYTHON = python$(PYTHON_VERSION)
VENV_PATH = $(shell poetry env use --quiet $(PYTHON); poetry env info --path)

SOURCEDIR = sqlthemall
TESTDIR = tests


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
	rm -rf _venv
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

test: bootstrap
	poetry run pytest -vvv --disable-warnings $(TESTDIR)

test_w_warnings: bootstrap
	poetry run pytest -vvv $(TESTDIR)

test-all:
	tox

coverage: bootstrap
	poetry run pytest -vvv --cov=$(SOURCEDIR) --disable-warnings --cov-report=html:coverage $(TESTDIR)

black:
	poetry run black --line-length 79 --safe $(SOURCEDIR) $(TESTDIR)


autoflake:
	poetry run autoflake -v -v --in-place --recursive --remove-all-unused-imports --ignore-init-module-imports $(SOURCEDIR) $(TESTDIR)

isort:
	poetry run isort $(SOURCEDIR) $(TESTDIR)

mypy:
	poetry run mypy --install-types --non-interactive --ignore-missing-imports $(SOURCEDIR) $(TESTDIR)

pdocstr:
	poetry run pydocstringformatter --linewrap-full-docstring --write  --max-line-length 79 $(SOURCEDIR) $(TESTDIR)

flake:
	poetry run flake8 --statistics --show-source --extend-ignore=S101,I900,G004,S310 $(SOURCEDIR) $(TESTDIR)

pylint:
	poetry run pylint --rcfile .pylintrc $(SOURCEDIR) $(TESTDIR)


lint: bootstrap black autoflake isort mypy pdocstr flake pylint



bandit:
	bandit -r $(SOURCEDIR)

##.PHONY: upload
#
##upload: test-all build-dist
##	_venv/bin/twine upload dist/*
##	make clean
#
#.PHONY: build-dist
#
#build-dist: clean
#	_venv/bin/pyproject-build
#
#.PHONY: clean
#
#
#update_req: bootstrap
#	sh -c '. _venv/bin/activate; REQILE="requirements-dev.txt"; cat "${REQILE}" | xargs --max-args=1 --delimiter='\n' python3 -m pip install -U; _cat "${REQILE}" | sed -e 's/[<>=]\+.*//' -e 's/^/^/' -e 's/$/[=]/g' > "_${REQILE}"; _python3 -m pip list --format=freeze | grep -f "_${REQILE}" > "${REQILE}"'
#	sh -c '. _venv/bin/activate; REQILE="requirements.txt"; cat "${REQILE}" | xargs --max-args=1 --delimiter='\n' python3 -m pip install -U; _cat "${REQILE}" | sed -e 's/[<>=]\+.*//' -e 's/^/^/' -e 's/$/[=]/g' > "_${REQILE}"; _python3 -m pip list --format=freeze | grep -f "_${REQILE}" > "${REQILE}"'


