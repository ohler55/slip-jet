
all: build

clean:
	rm *.so

lint:
	golangci-lint run

build:
	go build -buildmode=plugin -o jet.so *.go

test: lint
	make -C jet test

.PHONY: all build
