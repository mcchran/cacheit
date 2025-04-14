all: deps, test

deps:
	pip install -r requirements.txt

test:
	python -m unittest discover -s . -p "test_*.py"

clean:
	rm -rf __pycache__
	rm -rf *.egg-info
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .coverage
	rm -rf coverage.xml
	rm -rf htmlcov
	rm -rf .tox
	rm -rf .hypothesis
	rm -rf .pytest_cache
	rm -rf .coverage.*
	rm -rf coverage.*