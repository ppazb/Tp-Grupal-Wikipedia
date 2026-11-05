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
	$(MAKE) test-serial

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

# Levanta el sistema y espera la finalización de todos los clientes.
up: 
	bash scripts/ejecutar_sistema.sh $(CLIENTES)

# Baja el despliegue y borra los volúmenes.
down:
	$(COMPOSE) --profile clientes down -v --remove-orphans

.PHONY: comparar-serial test-serial
comparar-serial:
	bash scripts/comparar_serial.sh

test-serial:
	docker build -f scripts/serial/Dockerfile -t wikipedia-comparacion-serial .
	docker run --rm --entrypoint python -v "$(CURDIR):/proyecto:ro" wikipedia-comparacion-serial -m unittest discover -s tests/serial -v
