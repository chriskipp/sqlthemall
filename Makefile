# Some simple testing tasks (sorry, UNIX only).

FLAGS=


flake:
	autoflake -v -v --in-place --recursive --remove-all-unused-imports --ignore-init-module-imports .

isort:
	isort .
	mypy --ignore-missing-imports .

black:
	black --safe --exclude '__pycache__' --verbose .

test:
	pytest -vvv --disable-warnings tests

cov:
	pytest --cov=sqlthemall --cov-report=html:coverage --disable-warnings -vvv tests

clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -f `find . -type f -name '@*' `
	rm -f `find . -type f -name '#*#' `
	rm -f `find . -type f -name '*.orig' `
	rm -f `find . -type f -name '*.rej' `
	rm -f .coverage
	rm -rf coverage
	rm -rf build
	rm -rf htmlcov
	rm -rf dist

clean_up:
	rm -f -r .mypy_cache/ .pytest_cache/

.PHONY: flake isort black clean clean_up  #test

