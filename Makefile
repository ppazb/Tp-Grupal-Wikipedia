.DEFAULT_GOAL := test
.PHONY: test test-q5 generar-compose up down
CLIENTES ?= 1
COMPOSE = docker compose -f docker-compose.yaml

generar-compose:
	python3 scripts/generar_compose.py --clientes $(CLIENTES)

test:
	bash scripts/probar_escenarios.sh todos

test-q5:
	bash scripts/probar_escenarios.sh q5

# Levanta el sistema y espera la finalización de todos los clientes.
up:
	$(MAKE) down
	bash scripts/ejecutar_sistema.sh $(CLIENTES)

# Baja el despliegue y borra los volúmenes.
down:
	$(COMPOSE) --profile clientes down -v --remove-orphans

.PHONY: comparar-serial
comparar-serial:
	bash scripts/comparar_serial.sh
