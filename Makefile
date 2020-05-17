VENV_NAME = venv
VENV_ACTIVATE = $(VENV_NAME)/bin/activate
PYTHON = $(VENV_NAME)/bin/python3
COVERAGE =  $(VENV_NAME)/bin/coverage3

test: venv
	@. $(VENV_ACTIVATE); $(COVERAGE) run -m unittest discover -s tests
	@. $(VENV_ACTIVATE); $(COVERAGE) report

package: venv test
	@. $(VENV_ACTIVATE); \
		$(PYTHON) setup.py sdist bdist_wheel

venv: $(VENV_ACTIVATE)

$(VENV_ACTIVATE):
	@test -d $(VENV_NAME) || virtualenv -p python3 $(VENV_NAME)
	@. $(VENV_ACTIVATE); pip3 install -r requirements.txt
	@touch $(VENV_ACTIVATE)

install:
	@pip3 install .

uninstall: clean
	@pip3 uninstall $(PACKAGE_NAME)

clean:
	@rm -rf $(VENV_NAME)
	@rm -rf *.egg-info
	@rm -rf build/
	@rm -rf dist/
	@rm -rf __pycache__
	@rm -rf **/__pycache__
