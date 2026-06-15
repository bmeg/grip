PG_VERSION ?= pg15
PG_CONFIG ?= pg_config

.PHONY: help install-cargo-pgrx pgrx-init install run test psql

help:
	@echo "Targets:"
	@echo "  install-cargo-pgrx  Install cargo-pgrx"
	@echo "  pgrx-init           Initialize pgrx using PG_CONFIG=$(PG_CONFIG)"
	@echo "  install             Install the extension into the local pgrx postgres"
	@echo "  run                 Start the local pgrx postgres and open psql"
	@echo "  test                Run pgrx tests"
	@echo "  psql                Open psql against the default pgrx database"

install-cargo-pgrx:
	cargo install --locked cargo-pgrx

pgrx-init:
	cargo pgrx init --$(PG_VERSION) $(PG_CONFIG)

install:
	cargo pgrx install $(PG_VERSION)

run:
	cargo pgrx run $(PG_VERSION)

test:
	cargo pgrx test $(PG_VERSION)

psql:
	psql -d $(notdir $(CURDIR))