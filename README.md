# DeferredFutures

[![Build Status](https://travis-ci.org/invenia/DeferredFutures.jl.svg?branch=master)](https://travis-ci.org/invenia/DeferredFutures.jl)
[![Build status](https://ci.appveyor.com/api/projects/status/5sp5i4ewkfgw4cum/branch/master?svg=true)](https://ci.appveyor.com/project/iamed2/deferredfutures-jl/branch/master)
[![codecov](https://codecov.io/gh/invenia/DeferredFutures.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/invenia/DeferredFutures.jl)

A `DeferredFuture` is like a regular Julia `Future`, but is initialized when `put!` is called on it.
This means that the data in the `DeferredFuture` lives with the process the data was created on. 
The process the `DeferredFuture` itself lives on never needs to fetch the data to its process.
This is useful when there is a lightweight controller process which handles scheduling work on and transferring data between multiple machines.

## Usage

Use a `DeferredFuture` as you would a `Future`.

Note that `DeferredFuture(n)` does not control where the data lives, only where the `Future` which refers to the data lives. 

## License

DeferredFutures.jl is provided under the [MIT "Expat" License](LICENSE.md).
