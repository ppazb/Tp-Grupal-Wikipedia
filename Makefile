.PHONY: test test-middleware test-eof test-q4 test-sigterm test-gateway-q1
test:
	bash scripts/probar_escenarios.sh todos

test-middleware:
	bash scripts/probar_escenarios.sh middleware

test-eof:
	bash scripts/probar_escenarios.sh eof

test-q4:
	bash scripts/probar_escenarios.sh q4

test-sigterm:
	bash scripts/probar_escenarios.sh sigterm

# Pipeline completo cliente -> gateway -> Q1 con el compose principal.
# Falla si el cliente no termina bien; siempre baja el despliegue al terminar.
test-gateway-q1:
	docker compose down -v
	docker compose up -d --build --wait rabbitmq inicializador gateway date_router q1_filter_lang q1_formatter
	docker compose run --no-deps --rm client; status=$$?; docker compose down -v; exit $$status
