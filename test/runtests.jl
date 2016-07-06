using DeferredFutures
using Base.Test


@testset "Comparison" begin
    f = Future()

    @test DeferredFuture(f) == DeferredFuture(f)
    @test hash(DeferredFuture(f)) == hash(DeferredFuture(f))
end

@testset "Distributed" begin
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
