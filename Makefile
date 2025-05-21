build:
	python -m build .

install build:
	pip install --upgrade .[build]

install test:
	pip install --upgrade .[test]

install dev:
	pip install --upgrade .

test:
	python -m pytest

update version:
	echo "Update version to $(version)"
	sed -i "s/^version = .*/version = \"$(version)\"/" pyproject.toml
	sed -i -r "s/v[0-9]+\.[0-9]+\.[0-9]+/v$(version)/" README.md 
	sed -i -r "s/dremio-[0-9]+\.[0-9]+\.[0-9]+/dremio-$(version)/" README.md 
	sed -i -r "s/v[0-9]+\.[0-9]+\.[0-9]+/v$(version)/" docs/GETTING_STARTED.md 
	sed -i -r "s/dremio-[0-9]+\.[0-9]+\.[0-9]+/dremio-$(version)/" docs/GETTING_STARTED.md  

