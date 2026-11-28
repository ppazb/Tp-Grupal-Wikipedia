.DEFAULT_GOAL := test
.PHONY: test test-q5 generar-compose up up-chaos chaos down comparar-serial
CLIENTES ?= 1
CLIENTES_CHAOS ?= 2
INTERVALO ?= 12
CANTIDAD ?= 1
CHAOS_ARGS ?=
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

# Escenario automatizado: levanta el sistema e inyecta fallas periódicas con Chaos Monkey.
up-chaos:
	$(MAKE) generar-compose CLIENTES=$(CLIENTES_CHAOS)
	$(MAKE) down
	CHAOS_ARGS="$(CHAOS_ARGS)" bash scripts/ejecutar_con_chaos.sh $(CLIENTES_CHAOS) $(INTERVALO) $(CANTIDAD)

# Ejecuta Chaos Monkey manualmente contra un despliegue ya levantado.
chaos:
	python3 scripts/chaos_monkey.py --espera-inicial 0 --intervalo $(INTERVALO) --cantidad $(CANTIDAD) $(CHAOS_ARGS)

# Baja el despliegue y borra los volúmenes.
down:
	$(COMPOSE) --profile clientes down -v --remove-orphans

comparar-serial:
	bash scripts/comparar_serial.sh
