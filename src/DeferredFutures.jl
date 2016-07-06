module DeferredFutures

export DeferredFuture

using AutoHashEquals

@auto_hash_equals immutable DeferredFuture <: Base.AbstractRemoteRef
    outer::Future
end

DeferredFuture() = DeferredFuture(Future())
DeferredFuture(pid::Integer) = DeferredFuture(Future(pid))

function Base.put!(df::DeferredFuture, val)
    rc = RemoteChannel()
    put!(rc, val)
    put!(df.outer, rc)
    return df
end

function Base.isready(df::DeferredFuture)
    isready(df.outer) && isready(fetch(df.outer))
end

function Base.fetch(df::DeferredFuture)
    fetch(fetch(df.outer))
end

function Base.wait(df::DeferredFuture)
    wait(df.outer)
    wait(fetch(df.outer))
    return df
end

end # module
