.DEFAULT_GOAL := test
.PHONY: test test-middleware test-eof test-q4 test-q5 test-sigterm test-sigterm-despliegue test-compose generar-compose up up-quiet down
CLIENTES ?= 1
COMPOSE = docker compose -f docker-compose.yaml

generar-compose:
	python3 scripts/generar_compose.py --clientes $(CLIENTES)

test-compose:
	python3 -m unittest tests.compose.escenarios -v

test:
	bash scripts/probar_escenarios.sh todos

test-middleware:
	bash scripts/probar_escenarios.sh middleware

test-eof:
	bash scripts/probar_escenarios.sh eof

test-q4:
	bash scripts/probar_escenarios.sh q4

test-q5:
	bash scripts/probar_escenarios.sh q5

test-sigterm:
	bash scripts/probar_escenarios.sh sigterm

test-sigterm-despliegue:
	bash scripts/probar_escenarios.sh despliegue

# Levanta todo el sistema (workers, gateway y clientes) y muestra los logs.
up: generar-compose
	$(COMPOSE) --profile clientes up --build

# Pipeline completo con un solo cliente, mostrando solo los logs del cliente (EOF y tiempo total).
# Los resultados quedan en output/client_0. Falla si el cliente no termina bien;
# siempre baja el despliegue al terminar.
up-quiet: generar-compose
	$(COMPOSE) --profile clientes down -v --remove-orphans
	$(COMPOSE) up -d --build --wait
	$(COMPOSE) build client
	$(COMPOSE) run --no-deps --rm client; status=$$?; $(COMPOSE) --profile clientes down -v --remove-orphans; exit $$status

# Baja el despliegue y borra los volúmenes.
down:
	$(COMPOSE) --profile clientes down -v --remove-orphans
