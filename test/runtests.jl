using DeferredFutures
using Base.Test


@testset "DeferredFuture Comparison" begin
    fut = Future()
    @test DeferredFuture(fut) == DeferredFuture(fut)
    @test hash(DeferredFuture(fut)) == hash(DeferredFuture(fut))
end

@testset "DeferredChannel Comparison" begin
    fut = Future()
    func = () -> RemoteChannel()
    @test DeferredChannel(fut, func) == DeferredChannel(fut, func)
    @test hash(DeferredChannel(fut, func)) == hash(DeferredChannel(fut, func))
end

@testset "Distributed DeferredFuture" begin
    top = myid()
    bottom = addprocs(1)[1]
    @everywhere using DeferredFutures

    try
        val = "hello"
        df = DeferredFuture(top)

        @test !isready(df)

        fut = remotecall_wait(bottom, df) do dfr
            put!(dfr, val)
        end
        @test fetch(fut) == df
        @test isready(df)
        @test fetch(df) == val
        @test wait(df) == df
        @test_throws ErrorException put!(df, val)

        @test df.outer.where == top
        @test fetch(df.outer).where == bottom
    finally
        rmprocs(bottom)
    end
end

@testset "Distributed DeferredChannel" begin
    top = myid()
    bottom = addprocs(1)[1]
    @everywhere using DeferredFutures

    try
        val = "hello"
        channel = DeferredChannel(top)

        @test !isready(channel)

        fut = remotecall_wait(bottom, channel) do dfr
            put!(dfr, val)
        end
        @test fetch(fut) == channel
        @test isready(channel)
        @test fetch(channel) == val
        @test wait(channel) == channel

        put!(channel, "world")
        @test take!(channel) == val
        @test fetch(channel) == "world"

        @test channel.outer.where == top
        @test fetch(channel.outer).where == bottom
    finally
        rmprocs(bottom)
    end
end


@testset "Allocation" begin
    rand_size = 800000000  # sizeof(rand(10000, 10000))
    gc()
    main_size = Base.summarysize(Main)

    top = myid()
    bottom = addprocs(1)[1]
    @everywhere using DeferredFutures

    try
        df = DeferredFuture(top)

        remote_size = remotecall_fetch(bottom, df) do dfr
            gc()
            main_size = Base.summarysize(Main)
            put!(dfr, rand(10000, 10000))
            main_size
        end

        gc()
        @test Base.summarysize(Main) < main_size + rand_size

        remote_size_new = remotecall_fetch(bottom) do
            gc()
            Base.summarysize(Main)
        end

        @test remote_size_new >= remote_size + rand_size
    finally
        rmprocs(bottom)
    end
end

@testset "Transfer" begin
    rand_size = 800000000  # sizeof(rand(10000, 10000))
    gc()
    main_size = Base.summarysize(Main)

    top = myid()
    left, right = addprocs(2)
    @everywhere using DeferredFutures

    try
        df = DeferredFuture(top)

        left_remote_size = remotecall_fetch(left, df) do dfr
            gc()
            main_size = Base.summarysize(Main)
            put!(dfr, rand(10000, 10000))
            main_size
        end

        right_remote_size = remotecall_fetch(right, df) do dfr
            gc()
            main_size = Base.summarysize(Main)
            global data = fetch(dfr)
            main_size
        end

        gc()
        @test Base.summarysize(Main) < main_size + rand_size

        right_remote_size_new = remotecall_fetch(right) do
            gc()
            Base.summarysize(Main)
        end

        @test right_remote_size_new >= right_remote_size + rand_size
    finally
        rmprocs([left, right])
    end
end

@testset "@defer" begin
    channel = eval(macroexpand(quote
        @defer RemoteChannel(()->Channel(5))
    end))
    other_channel = eval(macroexpand(quote
        @defer RemoteChannel()
    end))

    put!(channel, 1)
    put!(channel, 2)

    @test fetch(channel) == 1
    @test take!(channel) == 1
    @test fetch(channel) == 2

    fut = eval(macroexpand(quote
        @defer Future()
    end))
    other_future = eval(macroexpand(quote
        @defer Future()
    end))

    try
        bad_channel = eval(macroexpand(quote
            @defer Channel()
        end))
    catch exc
        @test isa(exc, AssertionError)
    end

    close(channel)
end
