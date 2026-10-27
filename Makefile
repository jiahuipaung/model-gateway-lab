DATABASE_URL ?= postgres://gateway:gateway@127.0.0.1:55432/model_gateway?sslmode=disable
GO_ENV := GOCACHE=/tmp/model-gateway-go-cache GOMODCACHE=/tmp/model-gateway-mod-cache

.PHONY: test test-integration vet verify up down demo

test:
	$(GO_ENV) go test ./...

test-integration:
	docker compose stop gateway >/dev/null 2>&1 || true
	docker compose up -d --wait postgres
	$(GO_ENV) TEST_DATABASE_URL='$(DATABASE_URL)' go test -tags=integration ./internal/postgres -count=1

vet:
	$(GO_ENV) go vet ./...

verify:
	test -z "$$(gofmt -l .)"
	$(MAKE) vet
	$(MAKE) test
	$(MAKE) test-integration

up:
	docker compose up --build -d --wait

down:
	docker compose down

demo:
	./scripts/demo.sh

