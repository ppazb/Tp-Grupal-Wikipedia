.PHONY: test test-middleware test-eof up up-quiet down
test:
	bash scripts/probar_escenarios.sh todos

test-middleware:
	bash scripts/probar_escenarios.sh middleware

test-eof:
	bash scripts/probar_escenarios.sh eof

# Levanta todo el sistema (workers, gateway y clientes) y muestra los logs.
up:
	docker compose up --build

# Pipeline completo con un solo cliente, mostrando solo los logs del cliente (EOF y tiempo total).
# Los resultados quedan en output/client_0. Falla si el cliente no termina bien;
# siempre baja el despliegue al terminar.
up-quiet:
	docker compose down -v
	docker compose up -d --build --wait rabbitmq inicializador gateway date_router q1_filter_lang q1_formatter q2_filter_bio q2_image_downloader word_occurrences q3_intersection q4_frequency q4_top_n q5_average q5_comparator
	docker compose build client
	docker compose run --no-deps --rm client; status=$$?; docker compose down -v; exit $$status

# Baja el despliegue y borra los volúmenes.
down:
	docker compose down -v
