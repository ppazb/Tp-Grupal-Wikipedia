.PHONY: test test-middleware test-eof
test:
	bash scripts/probar_escenarios.sh todos

test-middleware:
	bash scripts/probar_escenarios.sh middleware

test-eof:
	bash scripts/probar_escenarios.sh eof
