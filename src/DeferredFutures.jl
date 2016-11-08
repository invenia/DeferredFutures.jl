module DeferredFutures

export @defer, DeferredChannel, DeferredFuture

using AutoHashEquals

abstract DeferredRemoteRef <: Base.AbstractRemoteRef

@auto_hash_equals immutable DeferredFuture <: DeferredRemoteRef
    outer::Future
end

DeferredFuture(pid::Integer=myid()) = DeferredFuture(Future(pid))

@auto_hash_equals immutable DeferredChannel <: DeferredRemoteRef
    outer::Future
    func::Function  # Channel generating function used for creating the `RemoteChannel`
end

function DeferredChannel(f::Function, pid::Integer=myid())
    DeferredChannel(Future(pid), f)
end

function DeferredChannel(pid::Integer=myid(), num::Integer=32; content::DataType=Any)
    DeferredChannel(Future(pid), ()->Channel{content}(num))
end


function Base.put!(ref::DeferredFuture, val)
    inner = RemoteChannel()
    put!(ref.outer, inner)
    put!(fetch(ref.outer), val)

    return ref
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

Base.getindex(ref::DeferredRemoteRef) = getindex(fetch(ref.outer))
Base.close(ref::DeferredChannel) = close(fetch(ref.outer))
Base.take!(ref::DeferredChannel) = take!(fetch(ref.outer))

macro defer(ex::Expr)
    if ex.head != :call
        throw(AssertionError("Expected expression to be a function call, but got $(ex)."))
    end

    if !isa(ex.args[1], Symbol)
        throw(AssertertionError("First argument to :call is not a Symbol."))
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
