# Makefile for Python project

# Python interpreter
PYTHON := python3

# Virtual environment
VENV := venv
VENV_ACTIVATE := $(VENV)/bin/activate

# Source directory
SRC_DIR := traffic_analyzer

# Test directory
TEST_DIR := tests

# Default target
.PHONY: all
all: setup lint test

# Create virtual environment
$(VENV)/bin/activate:
	$(PYTHON) -m venv $(VENV)

# Install dependencies
.PHONY: setup
setup: $(VENV)/bin/activate
	. $(VENV_ACTIVATE) && pip install -r requirements.txt

# Run tests
.PHONY: test
test: setup
	. $(VENV_ACTIVATE) && pytest $(TEST_DIR)

# Build the project
.PHONY: build
build: setup
	. $(VENV_ACTIVATE) && python setup.py build

# Install traffic_analyzer to /usr/local/bin
.PHONY: install
install: package
	. $(VENV_ACTIVATE) && pip install --upgrade --force-reinstall dist/*.whl
	sudo rm -f /usr/local/bin/traffic_analyzer
	sudo cp $(VENV)/bin/traffic_analyzer /usr/local/bin/traffic_analyzer
	sudo chmod +x /usr/local/bin/traffic_analyzer

# Package binary as installer
.PHONY: package
package: build
	. $(VENV_ACTIVATE) && python setup.py bdist_wheel

# Clean up
.PHONY: clean
clean:
	rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -delete


# Linter
LINTER := flake8

# Linter check
.PHONY: lint
lint: setup
	. $(VENV_ACTIVATE) && $(LINTER) --max-line-length=120 $(SRC_DIR) $(TEST_DIR)

# Auto-fix linting issues
.PHONY: lint-fix
lint-fix: setup
	. $(VENV_ACTIVATE) && autopep8 --in-place --recursive $(SRC_DIR) $(TEST_DIR)