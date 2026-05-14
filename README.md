# slip-jet

A NATS JetStream package for SLIP.

## Summary

This [Slip](https://github.com/ohler55/slip) package makes use of the
[NATS](https://nats.io) [JetStream Simplified Client
API](https://github.com/nats-io/nats.go/blob/main/jetstream/README.md#jetstream-simplified-client)
with the [Go
API](https://pkg.go.dev/github.com/nats-io/nats.go/jetstream). The
JetStream API is object based and this package uses Slip Flavors to
implement an object based API for Slip that closely follows the
JetStream API.

## Building

The package is implemented as a Go plugin. All plugins require that
the version of the code pulling in the plugin and the plugin version
match. That that means is the plugin must be build with the same
version of Slip in order to make use of the `require` Lisp function to
load the message package. It's actually a bit more finicky than that
though. The build must be done against the actual source code and not
simply putting the version in the go.mod requires. The
[slap](https://github.com/ohler55/slap) repo simplifies the process.

## Getting Started

 1. Install
 2. Run
 3. Explore

### Install

To make the slap application with the slip-jet plugin included,
checkout the [slap](https://github.com/ohler55/slap) repository and
build from the master branch by typing:

```
> make
```

The slap applicaiton in the top level directory ready to be used or
copied your choice of a `bin` directory.

### Run

Just run the slap application.

```
> slap
```

The Slip REPL will start and be ready for commands.

### Explore

A good way to explore the features of slip-message once in the slap
REPL is to use the `apropos` and `describe` function. A better way for
the slip-jet functions and flavors is to describe the jet package.

```lisp
▶ (describe *jet*)

```

Note that there are CLOS like functions for all the Flavor
methods. (Referred to as a Flavors CLOS belend or FLOS)
