build:
	py -m build .

install build:
	pip install --upgrade .[build]

install test:
	pip install --upgrade .[test]

install dev:
	pip install --upgrade .

test:
	python -m pytest