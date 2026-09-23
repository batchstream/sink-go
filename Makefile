.PHONY: test test-race test-integration lint clean

BIN_DIR := $(CURDIR)/.bin
STATICCHECK_VERSION := v0.8.1

test:
	go test ./... -count=1

test-race:
	go test -race ./... -count=1

test-integration:
	test -n "$$SINK_INTEGRATION_ADDRESS"
	go test -tags=integration ./... -count=1

lint:
	@test -z "$$(gofmt -l .)"
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) -checks=all ./...

clean:
	rm -r $(BIN_DIR)

# Core-package floors are kept separately from generated code and examples.
.PHONY: test-coverage
COVERAGE_DIR ?= .reports/coverage
test-coverage:
	@mkdir -p $(COVERAGE_DIR)
	go test -mod=readonly -race -covermode=atomic -coverpkg=./... -coverprofile=$(COVERAGE_DIR)/unit.out -count=1 -timeout=10m -json ./... > $(COVERAGE_DIR)/unit.jsonl
	python3 -m unittest discover -s scripts -p 'test_coverage.py'
	python3 scripts/check-coverage.py --profile $(COVERAGE_DIR)/unit.out --minimums .github/coverage-minimums.json --report $(COVERAGE_DIR)/summary.md
