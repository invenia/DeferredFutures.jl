using DeferredFutures
using Base.Test

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
