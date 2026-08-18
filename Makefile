.PHONY: install test producer

install:
	python -m pip install -e '.[test]'

test:
	pytest

producer:
	python -m src.producer.main --bootstrap-servers localhost:9092 --topic customer-events --count 100
