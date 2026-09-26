.PHONY: generate test lint bench cover

generate:
	buf lint && buf generate

test:
	go test -race -count=1 ./...

lint:
	go vet ./... && golangci-lint run ./... && buf lint

bench:
	go test -run '^$$' -bench . -benchmem ./...

cover:
	for pkg in . ./inspect; do go test -count=1 -coverprofile=coverage.out -coverpkg=$$pkg $$pkg && go tool cover -func=coverage.out | tail -1; done
