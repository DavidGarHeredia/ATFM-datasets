
using Test
using Queryverse
const testdir = dirname(@__FILE__)
include(testdir*"/../functions_step1.jl")


@testset "remove_flights_in_non_contiguous_USA" begin
	println()
	df = DataFrame(load(testdir*"/fakeFlightsForTests.csv"))
	territoriesToDelte = ["Alaska", "Hawaii"];
	@test nrow(df) == 6
	df_contiguous = remove_flights_in_non_contiguous_USA!(territoriesToDelte, df)
	@test nrow(df) == 6
	@test nrow(df_contiguous) == 3
end