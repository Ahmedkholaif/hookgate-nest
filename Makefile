# BROKER selects the broker for Compose: rabbitmq (default), sqs or pubsub.
BROKER   ?= rabbitmq
API_PORT ?= 3000
COMPOSE   = API_PORT=$(API_PORT) docker compose -f compose.yaml -f compose.$(BROKER).yaml
TEST_DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/hookgate_test

.PHONY: test up up-mixed down logs demo

test: ## unit + integration tests (needs Postgres at TEST_DATABASE_URL)
	TEST_DATABASE_URL=$(TEST_DATABASE_URL) npm test

up: ## Postgres + broker + api + 2 workers
	$(COMPOSE) up -d --build

up-mixed: ## also start 2 hookgate (Go) workers on the same queues (RabbitMQ)
	API_PORT=$(API_PORT) docker compose -f compose.yaml -f compose.rabbitmq.yaml -f compose.mixed.yaml up -d --build

down:
	$(COMPOSE) down -v --remove-orphans

logs:
	$(COMPOSE) logs -f api worker

demo: ## send signed webhooks (incl. duplicates and a failing one) and show the result
	scripts/demo.sh http://localhost:$(API_PORT)
