using Test
# include("../functions_step1.jl")
# include("../functions_step2.jl")
# include("../functions_step3.jl")
# include("../functions_step4.jl")

# const testdir = dirname(@__FILE__)


@testset "ATFM" begin
    for t in 1:4
        @info "test_functions_step$t"
        tp = joinpath(testdir, "$(t).jl")
        include(tp)
    end
end