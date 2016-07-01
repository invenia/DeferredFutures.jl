module DeferredFutures

export DeferredFuture

immutable DeferredFuture <: Base.AbstractRemoteRef
    outer::Future
end

Base.:(==)(df1::DeferredFuture, df2::DeferredFuture) = df1.outer == df2.outer

DeferredFuture(args...) = DeferredFuture(Future(args...))
DeferredFuture(pid::Integer) = DeferredFuture(Future(pid))

function Base.put!(df::DeferredFuture, val)
    inner = Future()
    put!(inner, val)
    put!(df.outer, inner)

    return df
end

Base.isready(df::DeferredFuture) = isready(df.outer) && isready(fetch(df.outer))
Base.fetch(df::DeferredFuture) = fetch(fetch(df.outer))
function Base.wait(df::DeferredFuture)
    wait(df.outer)
    wait(fetch(df.outer))
    return df
end

end # module
