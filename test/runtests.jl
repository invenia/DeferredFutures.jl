using DeferredFutures
using Base.Test

@testset "DeferredRemoteRefs" begin
    @testset "DeferredFuture Comparison" begin
        rc = RemoteChannel()
        @test DeferredFuture(rc) == DeferredFuture(rc)
        @test hash(DeferredFuture(rc)) == hash(DeferredFuture(rc))
    end

    @testset "DeferredChannel Comparison" begin
        rc = RemoteChannel()
        func = () -> RemoteChannel()
        @test DeferredChannel(rc, func) == DeferredChannel(rc, func)
        @test hash(DeferredChannel(rc, func)) == hash(DeferredChannel(rc, func))
    end

    @testset "Finalizing" begin
        df = DeferredFuture()
        finalize(df)
        @test_throws Exception isready(df)
        @test_throws Exception fetch(df)
        @test_throws Exception df[]
        @test_throws Exception put!(df, 1)
        @test_throws Exception take!(df)
        @test_throws Exception wait(df)
        finalize(df)

        dc = DeferredChannel()
        finalize(dc)
        @test_throws Exception isready(dc)
        @test_throws Exception fetch(dc)
        @test_throws Exception close(dc)
        @test_throws Exception dc[]
        @test_throws Exception put!(dc, 1)
        @test_throws Exception take!(dc)
        @test_throws Exception wait(dc)
        finalize(dc)

        dc = DeferredChannel()
        close(dc)
        @test !isready(dc)
        @test_throws Exception fetch(dc)
        @test_throws Exception dc[]
        @test_throws Exception put!(dc, 1)
        @test_throws Exception take!(dc)
        @test_throws Exception wait(dc)
        close(dc)
        finalize(dc)
        @test_throws Exception isready(dc)
        @test_throws Exception fetch(dc)
        @test_throws Exception close(dc)
        @test_throws Exception dc[]
        @test_throws Exception put!(dc, 1)
        @test_throws Exception take!(dc)
        @test_throws Exception wait(dc)

        df = DeferredFuture()
        put!(df, 1)
        @test df[] == 1
        finalize(df)
        @test_throws Exception isready(df)
        @test_throws Exception fetch(df)
        @test_throws Exception df[]
        @test_throws Exception put!(df, 1)
        @test_throws Exception take!(df)
        @test_throws Exception wait(df)

        dc = DeferredChannel()
        put!(dc, 1)
        @test dc[] == 1
        finalize(dc)
        @test_throws Exception isready(dc)
        @test_throws Exception fetch(dc)
        @test_throws Exception close(dc)
        @test_throws Exception dc[]
        @test_throws Exception put!(dc, 1)
        @test_throws Exception take!(dc)
        @test_throws Exception wait(dc)

        dc = DeferredChannel()
        put!(dc, 1)
        @test dc[] == 1
        close(dc)
        @test isready(dc)
        @test fetch(dc) == 1
        @test dc[] == 1
        @test_throws Exception put!(dc, 1)
        @test take!(dc) == 1
        @test !isready(dc)
        @test_throws Exception fetch(dc)
        @test_throws Exception dc[]
        @test_throws Exception put!(dc, 1)
        @test_throws Exception take!(dc)
        @test_throws Exception wait(dc)
        finalize(dc)
        @test_throws Exception isready(dc)
        @test_throws Exception fetch(dc)
        @test_throws Exception close(dc)
        @test_throws Exception dc[]
        @test_throws Exception put!(dc, 1)
        @test_throws Exception take!(dc)
        @test_throws Exception wait(dc)
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

            @test df[] == val
            @test df[5] == 'o'

            @test df.outer.where == top
            @test fetch(df.outer).where == bottom

            reset!(df)
            @test !isready(df)
            put!(df, "world")
            @test fetch(df) == "world"
            finalize(df)
            @test_throws Exception isready(df)
            @test_throws Exception fetch(df)
            @test_throws Exception df[]
            @test_throws Exception put!(df, 1)
            @test_throws Exception take!(df)
            @test_throws Exception wait(df)
            finalize(df)
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
            channel = DeferredChannel(top, 32)

            @test !isready(channel)

            fut = remotecall_wait(bottom, channel) do dfr
                put!(dfr, val)
            end
            @test fetch(fut) == channel
            @test isready(channel)
            @test fetch(channel) == val
            @test wait(channel) == channel

            @test channel[] == val
            @test channel[5] == 'o'

            put!(channel, "world")
            @test take!(channel) == val
            @test fetch(channel) == "world"

            @test channel.outer.where == top
            @test fetch(channel.outer).where == bottom

            reset!(channel)
            @test !isready(channel)
            put!(channel, "world")
            @test fetch(channel) == "world"
            finalize(channel)
            @test_throws Exception isready(channel)
            @test_throws Exception fetch(channel)
            @test_throws Exception close(channel)
            @test_throws Exception channel[]
            @test_throws Exception put!(channel, 1)
            @test_throws Exception take!(channel)
            @test_throws Exception wait(channel)
            finalize(channel)
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

                # the DeferredFuture is initialized and the data is stored on bottom
                put!(dfr, rand(10000, 10000))
                main_size
            end

            gc()
            # tests that the data has not been transfered to top
            @test Base.summarysize(Main) < main_size + rand_size

            remote_size_new = remotecall_fetch(bottom) do
                gc()
                Base.summarysize(Main)
            end

            # tests that the data still exists on bottom
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
        ex = macroexpand(:(@defer RemoteChannel(()->Channel(5))))
        ex = macroexpand(:(@defer RemoteChannel()))

        channel = @defer RemoteChannel(()->Channel(32))

        put!(channel, 1)
        put!(channel, 2)

        @test fetch(channel) == 1
        @test take!(channel) == 1
        @test fetch(channel) == 2

        fut = macroexpand(:(@defer Future()))
        other_future = macroexpand(:(@defer Future()))

        ex = macroexpand(:(@defer type Foo end))
        isa(ex.args[1], AssertionError)

        ex = macroexpand(:(@defer Channel()))
        isa(ex.args[1], AssertionError)

        close(channel)
    end

    @testset "Show" begin
        rc = RemoteChannel()
        rc_params = "($(rc.where),$(rc.whence),$(rc.id))"

        @test sprint(show, DeferredFuture(rc)) == "DeferredFuture at $rc_params"

        dc = DeferredChannel(rc, print)
        @test sprint(show, dc) == "DeferredChannel(print) at $rc_params"
    end

    @testset "Serialization" begin
        @testset "Serialization on same process" begin
            df = DeferredFuture(myid())
            io = IOBuffer()
            serialize(io, df)
            seekstart(io)
            deserialized_df = deserialize(io)
            close(io)
            @test deserialized_df == df
        end

        @testset "Serialization on a cluster" begin
            df = DeferredFuture(myid())

            io = IOBuffer()
            serialize(io, df)
            df_string = takebuf_string(io)
            close(io)

            bottom = addprocs(1)[1]
            @everywhere using DeferredFutures

            try
                @fetchfrom bottom begin
                    io = IOBuffer()
                    write(io, df_string)
                    seekstart(io)
                    bottom_df = deserialize(io)
                    put!(bottom_df, 37)
                end

                @test isready(df) == true
                @test fetch(df) == 37

            finally
                rmprocs(bottom)
            end
        end
    end
end
