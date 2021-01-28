

all:
	@echo 'available targets: install, clean, hello'

install:
	python3 -m venv ./venv
	. venv/bin/activate && pip install darshan

hello:
	. venv/bin/activate && python3 hello.py

clean:
	rm -rf venv
	rm -rf __pycache__
