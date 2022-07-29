.PHONY: env example

env:
	python -m venv .venv && \
	source .venv/bin/activate && \
	pip install --upgrade pip && \
	pip install -r requirements.txt

example:
	cp ./sample.profiles.yml ~/.dbt/profiles.yml && \
	echo "Please go to ~/.dbt/profiles.yml and fill out the missing config details"