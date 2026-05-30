.PHONY: install kafka-up kafka-down producer consumer dashboard clean

install:
	pip install -r requirements.txt

kafka-up:
	docker-compose up -d

kafka-down:
	docker-compose down

producer:
	python src/producer/stock_producer.py

consumer:
	python src/streaming/spark_streaming.py

dashboard:
	streamlit run src/dashboard/app.py

clean:
	rm -rf data/bronze/*
	rm -rf data/silver/*
	rm -rf data/gold/*
	rm -rf checkpoints/*
	rm -rf logs/*
