module DeferredFutures

export @defer, DeferredChannel, DeferredFuture, reset!

using AutoHashEquals

abstract DeferredRemoteRef <: Base.AbstractRemoteRef

@auto_hash_equals type DeferredFuture <: DeferredRemoteRef
    outer::RemoteChannel
end

function DeferredFuture(pid::Integer=myid())
    ref = DeferredFuture(RemoteChannel(pid))
    finalizer(ref, finalize_ref)
    return ref
end

@auto_hash_equals type DeferredChannel <: DeferredRemoteRef
    outer::RemoteChannel
    func::Function  # Channel generating function used for creating the `RemoteChannel`
end

function DeferredChannel(f::Function, pid::Integer=myid())
    ref = DeferredChannel(RemoteChannel(pid), f)
    finalizer(ref, finalize_ref)
    return ref
end

function DeferredChannel(pid::Integer=myid(), num::Integer=1; content::DataType=Any)
    ref = DeferredChannel(RemoteChannel(pid), ()->Channel{content}(num))
    finalizer(ref, finalize_ref)
    return ref
end

function finalize_ref(ref::DeferredRemoteRef)
    # finalizes as recommended in Julia docs:
    # http://docs.julialang.org/en/latest/manual/parallel-computing.html#Remote-References-and-Distributed-Garbage-Collection-1

    # check for ref.outer.where == 0 as the contained RemoteChannel may have already been
    # finalized
    if ref.outer.where > 0 && isready(ref.outer)
        inner = take!(ref.outer)

        finalize(inner)
    end

    finalize(ref.outer)

    return nothing
end

function reset!(ref::DeferredRemoteRef)
    if isready(ref.outer)
        inner = take!(ref.outer)

        # as recommended in Julia docs:
        # http://docs.julialang.org/en/latest/manual/parallel-computing.html#Remote-References-and-Distributed-Garbage-Collection-1
        finalize(inner)
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

function Base.close(ref::DeferredChannel)
    if isready(ref.outer)
        inner = fetch(ref.outer)
        close(inner)
    else
        rc = RemoteChannel()
        close(rc)

        put!(ref.outer, rc)
    end

    return nothing
end

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
