.PHONY: test test-unit test-integration test-e2e test-e2e-ui test-e2e-k8s test-stress test-all local-unit local-e2e lint format run migrate build-ui dev-ui dev dev-native dev-k8s dev-k8s-tf dev-k8s-tf-stop dev-stop infra-up infra-down setup setup-full docker-build docker-up docker-down docker-infra docker-app docker-logs docker-ps docker-clean tf-init tf-plan tf-apply tf-destroy

PYTHON ?= python3
PYTEST = $(PYTHON) -m pytest

test:
	$(PYTEST) de_platform/ tests/unit/ -v -m "not integration and not e2e" -n auto

test-unit:
	$(PYTEST) de_platform/ tests/unit/ -v -m "not integration and not e2e" -n auto

test-integration:
	$(PYTEST) tests/integration/ -v -m integration --tb=short

test-e2e:
	$(PYTEST) tests/e2e/ -v -m e2e --tb=short -n auto

test-e2e-ui:
	$(PYTEST) tests/e2e_ui/ -v -m e2e_ui --tb=short

test-e2e-k8s:
	$(PYTEST) tests/e2e_k8s/ -v -m e2e_k8s --tb=short

test-stress:
	$(PYTEST) tests/stress/ -v -m stress --tb=short

test-all:
	$(MAKE) test-unit
	$(MAKE) test-integration
	$(MAKE) test-e2e
	$(MAKE) test-e2e-ui

lint:
	$(PYTHON) -m ruff check de_platform/

format:
	$(PYTHON) -m ruff format de_platform/

run:
	$(PYTHON) -m de_platform run $(module) $(args)

migrate:
	$(PYTHON) -m de_platform migrate $(cmd) $(args)

local-unit: setup
	.venv/bin/python -m pytest de_platform/ tests/unit/ -v -m "not integration and not e2e"

local-e2e: setup-full infra-up
	.venv/bin/python -m pytest tests/e2e/ -v -m e2e --tb=short

infra-up:
	docker compose -f .devcontainer/docker-compose.yml up -d postgres redis minio zookeeper kafka clickhouse prometheus loki grafana

infra-down:
	docker compose -f .devcontainer/docker-compose.yml down

build-ui:
	cd ui && npm install && npm run build

dev-ui:
	cd ui && npm run dev

dev:
	./scripts/dev-docker.sh

dev-native:
	./scripts/dev-native.sh

dev-k8s:
	./scripts/dev-k8s.sh

dev-stop:
	@echo "Stopping Docker Compose services..."
	@docker compose --profile full down 2>/dev/null || true
	@echo "Deleting kind cluster (if any)..."
	@kind delete cluster --name de-platform 2>/dev/null || true
	@if [ -f .dev-pids ]; then \
		while IFS= read -r pid; do kill "$$pid" 2>/dev/null || true; done < .dev-pids; \
		rm -f .dev-pids; \
	fi
	@echo "All dev environments stopped."

setup:
	python3.12 -m venv .venv
	.venv/bin/pip install -e '.[dev]'

setup-full:
	python3.12 -m venv .venv
	.venv/bin/pip install -e '.[dev,infra]'

# ── Docker ─────────────────────────────────────────────────────────

docker-build:
	docker build -t de-platform:latest .

docker-up:
	docker compose --profile full up -d

docker-down:
	docker compose --profile full down

docker-infra:
	docker compose --profile infra up -d

docker-app:
	docker compose --profile app up -d

docker-logs:
	docker compose --profile full logs -f

docker-ps:
	docker compose --profile full ps

docker-clean:
	docker compose --profile full down -v

# ── Terraform K8s ──────────────────────────────────────────────────

tf-init:
	terraform -chdir=deploy/terraform-k8s init -input=false

tf-plan:
	terraform -chdir=deploy/terraform-k8s plan

tf-apply:
	terraform -chdir=deploy/terraform-k8s apply -auto-approve

tf-destroy:
	terraform -chdir=deploy/terraform-k8s destroy -auto-approve

dev-k8s-tf:
	./scripts/dev-k8s-tf.sh

dev-k8s-tf-stop:
	@echo "Destroying Terraform resources..."
	@terraform -chdir=deploy/terraform-k8s destroy -auto-approve 2>/dev/null || true
	@echo "Deleting kind cluster..."
	@kind delete cluster --name de-platform 2>/dev/null || true
	@if [ -f .dev-pids ]; then \
		while IFS= read -r pid; do kill "$$pid" 2>/dev/null || true; done < .dev-pids; \
		rm -f .dev-pids; \
	fi
	@echo "Terraform K8s environment stopped."
