.PHONY: generate test lint bench

generate:
	buf lint && buf generate

test:
	go test -race -count=1 ./...

lint:
	go vet ./... && buf lint

bench:
	go test -run '^$$' -bench . -benchmem ./...
