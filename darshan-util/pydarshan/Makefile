.PHONY: clean clean-test clean-pyc clean-build docs help


clean: clean-build clean-docs clean-pyc clean-test clean-modules  # remove all build, test, coverage and Python artifacts

clean-build:  # remove build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf .eggs/
	find . -name '*.egg-info' -exec rm -rf {} +
	find . -name '*.egg' -exec rm -rf {} +

clean-docs:
	rm -rf docs/build
	rm -f docs/darshan.rst
	rm -f docs/darshan.*.rst
	rm -f docs/modules.rst

clean-wheels:
	rm -rf wheelhouse

clean-pyc:  # remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -rf {} +
	find . -name '__pycache__' -exec rm -rf {} +

clean-test:  # remove test and coverage artifacts
	rm -rf .tox/
	rm -f .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache
	rm -rf pkgtest

clean-devenv:
	rm -rf devenv/venv
	rm -rf devenv/libdarshanutil
	rm -rf devenv

clean-modules:
	for file in `find ../../modules/ -name "*backend.py"` ; do \
		rm -rf darshan/backend/`basename $$file | cut -d '-' -f 1`.py ; \
	done

devenv:
	./devel/build-libdarshanutil.sh
	python3 -m venv devenv/venv
	source devenv/venv/bin/activate && pip install -r requirements_dev.txt

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


docs: clean-docs # generate Sphinx HTML documentation, including API docs
	sphinx-apidoc -M -H PyDarshan -o docs/api/pydarshan darshan
	$(MAKE) -C docs clean
	$(MAKE) -C docs html

docs-show:
	xdg-open docs/build/html/index.html

# add AutoPef Python packages...
add-modules:
	for file in `find ../../modules/ -name "*-backend.py"` ; do \
		cp $$file darshan/backend/`basename $$file | cut -d '-' -f 1`.py ; \
	done

wheels:
	./devel/build-all.sh

release: #dist # package and upload a release
	#twine upload --repository testpypi dist/*
	twine upload dist/*

dist: clean  # builds source and wheel package
	python3 setup.py sdist
	python3 setup.py bdist_wheel

	# might want to remove none-any wheel, but more specific wheels seem to take precedence
	# rm -r dist/*non-any.whl

	# gather binary wheels
	# pip >= 19.0
	find ./wheelhouse/manylinux2010* -name *manylinux*.whl -exec cp {} ./dist \;
	# pip >= 8.1.0, may overwrite previously copied, but these have greater compatability
	find ./wheelhouse/manylinux1* -name *manylinux*.whl -exec cp {} ./dist \;

	ls -l dist


install: clean  # install the package to the active Python's site-packages
	python3 setup.py install 
