.PHONY: help start stop restart status health logs clean pull \
        flink-sql producer topics-list connectors-list marquez-open

help:
	@echo "Comandos disponibles:"
	@echo "  make start            - Levanta el stack completo y lo configura"
	@echo "  make stop             - Detiene todos los contenedores"
	@echo "  make restart          - Reinicia el stack"
	@echo "  make status           - Estado de contenedores"
	@echo "  make health           - Diagnóstico completo"
	@echo "  make logs             - Logs en vivo de todos los servicios"
	@echo "  make clean            - Elimina contenedores + datos persistentes (irreversible)"
	@echo "  make flink-sql        - Abre el SQL Client de Flink interactivo"
	@echo "  make producer         - Envía una solicitud de hipoteca de ejemplo (John Doe)"
	@echo "  make topics-list      - Lista los topics de Kafka"
	@echo "  make connectors-list  - Lista los conectores de Kafka Connect"

start:
	bash scripts/start.sh

stop:
	docker compose stop

restart:
	docker compose restart

status:
	docker compose ps

health:
	bash scripts/health-check.sh

logs:
	docker compose logs -f

clean:
	docker compose down -v
	rm -rf data/kafka-data data/connect-plugins data/postgres-mortgage data/marquez-db

pull:
	docker compose pull

flink-sql:
	docker compose exec -it flink-sql-client bin/sql-client.sh

producer:
	python3 producer/submit_application.py --name "John Doe" \
		--property-value 200000 --loan-amount 150000 --annual-income 500000

topics-list:
	docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

connectors-list:
	curl -s http://localhost:8083/connectors | python3 -m json.tool

marquez-open:
	@echo "Abre http://localhost:3000 en tu navegador"
