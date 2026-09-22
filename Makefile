SHELL := /bin/bash
MODULE := github.com/udaykishore-resu/plantstream
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
IMAGE ?= ghcr.io/udaykishore-resu/plantstream
BIN := bin

PLANT_FILE ?= examples/plant.yaml
HTTP_ADDR ?= :8080
PLC_SIM_ADDR ?= :5020

.PHONY: help run run-modbus run-full stop-full build test lint vet cover docker helm-lint tidy gen fmt vuln clean

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

run: ## Run the edge node with zero infrastructure (in-memory broker, simulated sources)
	PLANTSTREAM_PLANT_FILE=$(PLANT_FILE) PLANTSTREAM_HTTP_ADDR=$(HTTP_ADDR) go run ./cmd/plantstream

run-modbus: ## Run plc-sim (Modbus TCP) and the edge node reading it
	@go run ./cmd/plc-sim -addr $(PLC_SIM_ADDR) & \
	SIM_PID=$$!; trap 'kill $$SIM_PID 2>/dev/null' EXIT; sleep 1; \
	PLANTSTREAM_PLANT_FILE=examples/plant-modbus.yaml PLANTSTREAM_HTTP_ADDR=$(HTTP_ADDR) go run ./cmd/plantstream

run-full: ## Full local stack via docker compose (Mosquitto, Kafka KRaft, OTel collector, plc-sim, plantstream)
	docker compose -f deploy/docker-compose.yaml up --build

stop-full: ## Tear down the docker compose stack
	docker compose -f deploy/docker-compose.yaml down -v

build: ## Build both binaries into ./bin
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/plantstream ./cmd/plantstream
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/plc-sim ./cmd/plc-sim

test: ## Run all tests with the race detector
	go test -race -count=1 -p 1 ./...

vet: ## go vet
	go vet ./...

lint: ## golangci-lint (install: https://golangci-lint.run)
	golangci-lint run ./...

fmt: ## gofmt all sources
	gofmt -s -w ./cmd ./internal

cover: ## Coverage report for the domain packages
	go test -race -count=1 -p 1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1
	@echo "open with: go tool cover -html=coverage.out"

vuln: ## govulncheck
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

docker: ## Build the container image
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

helm-lint: ## Lint the Helm chart
	helm lint deploy/helm/plantstream
	helm template plantstream deploy/helm/plantstream > /dev/null

tidy: ## go mod tidy
	go mod tidy

gen: ## Regenerate golden files
	UPDATE_GOLDEN=1 go test -count=1 ./internal/domain/uns/

clean: ## Remove build artefacts
	rm -rf $(BIN) coverage.out data/
