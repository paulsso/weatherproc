.PHONY: setup clean

VENV_DIR := venv
PYTHON := $(VENV_DIR)/bin/python
PIP := $(VENV_DIR)/bin/pip

setup: $(VENV_DIR)
	$(PIP) install -r requirements.txt

$(VENV_DIR):
	python3 -m venv $(VENV_DIR)

clean:
	rm -rf $(VENV_DIR) 