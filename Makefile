.PHONY: test test-middleware test-eof test-gateway-q1 test-q1-q2 test-q1-q2-q4-q5
test:
	bash scripts/probar_escenarios.sh todos

test-middleware:
	bash scripts/probar_escenarios.sh middleware

test-eof:
	bash scripts/probar_escenarios.sh eof

# Pipeline completo cliente -> gateway -> Q1 con el compose principal.
# Falla si el cliente no termina bien; siempre baja el despliegue al terminar.
test-gateway-q1:
	docker compose down -v
	docker compose up -d --build --wait rabbitmq inicializador gateway date_router q1_filter_lang q1_formatter
	docker compose build client
	docker compose run --no-deps --rm client; status=$$?; docker compose down -v; exit $$status

# Pipeline completo cliente -> gateway -> Q1 y Q2 con el compose principal.
# Los resultados quedan en output/client_0. Falla si el cliente no termina bien;
# siempre baja el despliegue al terminar.
test-q1-q2:
	docker compose down -v
	docker compose up -d --build --wait rabbitmq inicializador gateway date_router q1_filter_lang q1_formatter q2_filter_bio q2_image_downloader
	docker compose build client
	docker compose run --no-deps --rm client; status=$$?; docker compose down -v; exit $$status

# Pipeline completo cliente -> gateway -> Q1, Q2, Q4 y Q5 con el compose principal.
# no termina bien; siempre baja el despliegue al terminar.
test-q1-q2-q4-q5:
	docker compose down -v
	docker compose up -d --build --wait rabbitmq inicializador gateway date_router q1_filter_lang q1_formatter q2_filter_bio q2_image_downloader word_occurrences q4_frequency q4_top_n q5_average q5_comparator
	docker compose build client
	docker compose run --no-deps --rm client; status=$$?; docker compose down -v; exit $$status
