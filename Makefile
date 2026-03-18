.PHONY: help install test lint format clean run

help:
	@echo "Available commands:"
	@echo "  install     Install production dependencies"
	@echo "  test        Run tests"
	@echo "  lint        Run linting"
	@echo "  format      Format code"
	@echo "  clean       Clean cache files"
	@echo "  run         Run the application"

install:
	pip install -r requirements.txt

test:
	pytest

lint:
	flake8 .
	mypy src/

format:
	black .
	isort .

clean:
	find . -type d -name "__pycache__" -delete
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .coverage
	rm -rf htmlcov

run:
	python main.py
