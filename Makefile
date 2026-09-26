.PHONY: generate test lint bench cover

generate:
	buf lint && buf generate

test:
	go test -race -count=1 ./...

lint:
	go vet ./... && buf lint

bench:
	go test -run '^$$' -bench . -benchmem ./...

cover:
	go test -count=1 -coverprofile=coverage.out -coverpkg=. . && go tool cover -func=coverage.out | tail -1
