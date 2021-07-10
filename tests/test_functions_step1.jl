using Base: close_chnl_on_taskdone

using Test
using Queryverse
const testdir = dirname(@__FILE__)
include(testdir*"/../functions_step1.jl")

df_flights = DataFrame(load(testdir*"/fakeFlightsForTests.csv"))
df_airports = DataFrame(load(testdir*"/fakeAirportsForTests.csv"))
territoriesToDelte = ["Alaska", "Hawaii"];
@test nrow(df_flights) == 7

@testset "remove_flights_in_non_contiguous_USA" begin
	df_contiguous = remove_flights_in_non_contiguous_USA!(territoriesToDelte, df_flights)
	@test nrow(df_flights) == 7
	@test nrow(df_contiguous) == 4
	@test df_contiguous[1, :OriginStateName] == "California"
	@test df_contiguous[1, :DestStateName] == "California"
	@test df_contiguous[2, :OriginStateName] == "Fake"
	@test df_contiguous[2, :DestStateName] == "Fake"
	@test df_contiguous[3, :OriginStateName] == "Fake"
	@test df_contiguous[3, :DestStateName] == "Fake"
	@test df_contiguous[4, :OriginStateName] == "California"
	@test df_contiguous[4, :DestStateName] == "Idaho"
end

@testset "filter_observation_and_keep_only_useful_columns" begin
	df_filtered = filter_observation_and_keep_only_useful_columns(df_flights, 5)
	@test nrow(df_flights) == 7
	@test nrow(df_filtered) == 2
	@test df_filtered[1, :Tail_Number] == "tail1"
	@test df_filtered[2, :Tail_Number] == "tail4"
end

@testset "remove_flights_with_no_airport_in_the_data_set" begin
	df_removed = remove_flights_with_no_airport_in_the_data_set(df_airports, df_flights)
	@test nrow(df_flights) == 7
	@test nrow(df_removed) == 2
	@test df_removed[1, :OriginAirportID] == 14698
	@test df_removed[1, :DestAirportID] == 14771
	@test df_removed[2, :OriginAirportID] == 14771 
	@test df_removed[2, :DestAirportID] == 10713
end

@testset "clean_data_flights" begin
	df_final = clean_data_flights(df_flights, 5, territoriesToDelte, df_airports)
	@test nrow(df_flights) == 7
	@test nrow(df_final) == 2
	@test df_final[1, :OriginAirportID] == 14698
	@test df_final[1, :DestAirportID] == 14771
	@test df_final[2, :OriginAirportID] == 14771 
	@test df_final[2, :DestAirportID] == 10713
	@test df_final[1, :Tail_Number] == "tail1"
	@test df_final[2, :Tail_Number] == "tail4"
end