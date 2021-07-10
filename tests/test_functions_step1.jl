
using Test
using Queryverse
const testdir = dirname(@__FILE__)
include(testdir*"/../functions_step1.jl")


@testset "remove_flights_in_non_contiguous_USA" begin
	df = DataFrame(load(testdir*"/fakeFlightsForTests.csv"))
	territoriesToDelte = ["Alaska", "Hawaii"];
	@test nrow(df) == 6
	df_contiguous = remove_flights_in_non_contiguous_USA!(territoriesToDelte, df)
	@test nrow(df) == 6
	@test nrow(df_contiguous) == 3
end

@testset "filter_observation_and_keep_only_useful_columns" begin
	df = DataFrame(load(testdir*"/fakeFlightsForTests.csv"))
	@test nrow(df) == 6
	df_filtered = filter_observation_and_keep_only_useful_columns(df, 5)
	@test nrow(df) == 6
	@test nrow(df_filtered) == 2
	@test df_filtered[1, :Tail_Number] == "tail1"
	@test df_filtered[2, :Tail_Number] == "tail4"
end