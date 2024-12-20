
all: build

clean:
	rm -f *.so

lint:
	golangci-lint run

build:
	go mod tidy
	go build -buildmode=plugin -o jet.so *.go

test: lint
	make -C jet test

.PHONY: all build
