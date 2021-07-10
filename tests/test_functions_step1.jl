
using Test
using Queryverse
const testdir = dirname(@__FILE__)
include(testdir*"/../functions_step1.jl")


@testset "remove_flights_in_non_contiguous_USA" begin
	df = DataFrame(load(testdir*"/fakeFlightsForTests.csv"))
	territoriesToDelte = ["Alaska", "Hawaii"];
	@test nrow(df) == 7
	df_contiguous = remove_flights_in_non_contiguous_USA!(territoriesToDelte, df)
	@test nrow(df) == 7
	@test nrow(df_contiguous) == 4
end

@testset "filter_observation_and_keep_only_useful_columns" begin
	df = DataFrame(load(testdir*"/fakeFlightsForTests.csv"))
	@test nrow(df) == 7
	df_filtered = filter_observation_and_keep_only_useful_columns(df, 5)
	@test nrow(df) == 7
	@test nrow(df_filtered) == 2
	@test df_filtered[1, :Tail_Number] == "tail1"
	@test df_filtered[2, :Tail_Number] == "tail4"
end

@testset "remove_flights_with_no_airport_in_the_data_set" begin
	df_flights = DataFrame(load(testdir*"/fakeFlightsForTests.csv"))
	df_airports = DataFrame(load(testdir*"/fakeAirportsForTests.csv"))
	@test nrow(df_flights) == 7
	df_removed = remove_flights_with_no_airport_in_the_data_set(df_airports, df_flights)
	@test nrow(df_flights) == 7
	@test nrow(df_removed) == 2
	@test df_removed[1, :OriginAirportID] == 14698
	@test df_removed[1, :DestAirportID] == 14771
	@test df_removed[2, :OriginAirportID] == 14771 
	@test df_removed[2, :DestAirportID] == 10713
end