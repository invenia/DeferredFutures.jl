module DeferredFutures

export DeferredFuture

type DeferredFuture <: Base.AbstractRemoteRef
    outer::Future
end

function Base.:(==)(df1::DeferredFuture, df2::DeferredFuture)
    df1.outer == df2.outer
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
