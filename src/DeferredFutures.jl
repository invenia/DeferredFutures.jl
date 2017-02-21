module DeferredFutures

export @defer, DeferredChannel, DeferredFuture, reset!

using AutoHashEquals

abstract DeferredRemoteRef <: Base.AbstractRemoteRef

@auto_hash_equals immutable DeferredFuture <: DeferredRemoteRef
    outer::RemoteChannel
end

DeferredFuture(pid::Integer=myid()) = DeferredFuture(RemoteChannel(pid))

@auto_hash_equals immutable DeferredChannel <: DeferredRemoteRef
    outer::RemoteChannel
    func::Function  # Channel generating function used for creating the `RemoteChannel`
end

function DeferredChannel(f::Function, pid::Integer=myid())
    DeferredChannel(RemoteChannel(pid), f)
end

function DeferredChannel(pid::Integer=myid(), num::Integer=1; content::DataType=Any)
    DeferredChannel(RemoteChannel(pid), ()->Channel{content}(num))
end

function reset!(ref::DeferredRemoteRef)
    if isready(ref.outer)
        take!(ref.outer)
    end

    return ref
end

function Base.put!(ref::DeferredFuture, val)
    if !isready(ref.outer)
        inner = RemoteChannel()
        put!(ref.outer, inner)
        put!(fetch(ref.outer), val)

        return ref
    else
        throw(ErrorException("DeferredFuture can only be set once."))
    end
end

function Base.put!(ref::DeferredChannel, val)
    # On the first call to put! create the `RemoteChannel`
    # and `put!` it in the `Future`
    if !isready(ref.outer)
        inner = RemoteChannel(ref.func)
        put!(ref.outer, inner)
    end

    # `fetch` the `RemoteChannel` and `put!`
    # the value in there
    put!(fetch(ref.outer), val)

    return ref
end

function Base.isready(ref::DeferredRemoteRef)
    isready(ref.outer) && isready(fetch(ref.outer))
end

function Base.fetch(ref::DeferredRemoteRef)
    fetch(fetch(ref.outer))
end

function Base.wait(ref::DeferredRemoteRef)
    wait(ref.outer)
    wait(fetch(ref.outer))
    return ref
end

# mimics the Future/RemoteChannel indexing behaviour in Base
Base.getindex(ref::DeferredRemoteRef, args...) = getindex(fetch(ref.outer), args...)
Base.close(ref::DeferredChannel) = close(fetch(ref.outer))
Base.take!(ref::DeferredChannel) = take!(fetch(ref.outer))

macro defer(ex::Expr)
    if ex.head != :call
        throw(AssertionError("Expected expression to be a function call, but got $(ex)."))
    end

    if ex.args[1] == :Future
        return Expr(:call, :DeferredFuture, ex.args[2:end]...)
    elseif ex.args[1] == :RemoteChannel
        return Expr(:call, :DeferredChannel, ex.args[2:end]...)
    else
        throw(AssertionError("Expected RemoteChannel or Future and got $(ex.args[1])."))
    end
end

end # module
