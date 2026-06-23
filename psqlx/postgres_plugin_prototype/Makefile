PG_MAJOR ?= 15
COMPOSE ?= docker compose
PACKAGE_DIR ?= build/pg$(PG_MAJOR)-package

.PHONY: build-plugin postgres-image up down reset logs psql smoke-test

build-plugin:
	$(COMPOSE) build builder
	$(COMPOSE) run --rm builder ./scripts/build_extension.sh $(PACKAGE_DIR)

postgres-image: build-plugin
	$(COMPOSE) build postgres

up: postgres-image
	$(COMPOSE) up -d postgres

down:
	$(COMPOSE) down --remove-orphans

reset:
	$(COMPOSE) down -v --remove-orphans

logs:
	$(COMPOSE) logs -f postgres

psql:
	$(COMPOSE) exec postgres psql -U postgres -d grip_plugin_dev

smoke-test:
	./scripts/smoke_test.sh