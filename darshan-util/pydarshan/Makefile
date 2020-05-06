.PHONY: clean clean-test clean-pyc clean-build docs help


clean: clean-build clean-pyc clean-test  # remove all build, test, coverage and Python artifacts

clean-build:  # remove build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf .eggs/
	rm -rf docs/_build
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc:  # remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:  # remove test and coverage artifacts
	rm -rf .tox/
	rm -f .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache


lint:  # check style with flake8
	flake8 darshan tests

test:  # run tests quickly with the default Python
	py.test --verbose

test-all:  # run tests on every Python version with tox
	tox

coverage:  # check code coverage quickly with the default Python
	coverage run --source darshan -m pytest
	coverage report -m
	coverage html
	xdg-open htmlcov/index.html


docs:  # generate Sphinx HTML documentation, including API docs
	rm -f docs/darshan.rst
	rm -f docs/darshan.backend.rst
	rm -f docs/darshan.plots.rst
	rm -f docs/modules.rst
	sphinx-apidoc -o docs/ darshan
	$(MAKE) -C docs clean
	$(MAKE) -C docs html

docs-show: docs
	xdg-open docs/_build/html/index.html

servedocs: docs  # compile the docs watching for changes
	watchmedo shell-command -p '*.rst' -c '$(MAKE) -C docs html' -R -D .


release: dist # package and upload a release
	twine upload --repository testpypi dist/*
	#twine upload dist/*

dist: clean  # builds source and wheel package
	python setup.py sdist
	python setup.py bdist_wheel
	ls -l dist


install: clean  # install the package to the active Python's site-packages
	python setup.py install
