
using Test
using Queryverse
const testdir = dirname(@__FILE__)
include(testdir*"/../functions_step1.jl")

df_flights = DataFrame(load(testdir*"/fakeFlightsForTests.csv"))
df_airports = DataFrame(load(testdir*"/fakeAirportsForTests.csv"))
territoriesToDelte = ["Alaska", "Hawaii"];
@test nrow(df_flights) == 7

@testset "remove_flights_in_non_contiguous_USA" begin
	df_contiguous = remove_flights_in_non_contiguous_USA(territoriesToDelte, df_flights)
	@test nrow(df_flights) == 7
	@test nrow(df_contiguous) == 4
	@test df_contiguous[1, :OriginStateName] == "California"
	@test df_contiguous[1, :DestStateName]   == "California"
	@test df_contiguous[2, :OriginStateName] == "Fake"
	@test df_contiguous[2, :DestStateName]   == "Fake"
	@test df_contiguous[3, :OriginStateName] == "Fake"
	@test df_contiguous[3, :DestStateName]   == "Fake"
	@test df_contiguous[4, :OriginStateName] == "California"
	@test df_contiguous[4, :DestStateName]   == "Idaho"
end

@testset "filter_observation_and_keep_only_useful_columns" begin
	df_filtered = filter_observation_and_keep_only_useful_columns(df_flights, 5)
	@test nrow(df_flights)  == 7
	@test nrow(df_filtered) == 2
	@test df_filtered[1, :Tail_Number] == "tail1"
	@test df_filtered[2, :Tail_Number] == "tail4"
end

@testset "remove_flights_with_no_airport_in_the_data_set" begin
	df_removed = remove_flights_with_no_airport_in_the_data_set(df_airports, df_flights)
	@test nrow(df_flights) == 7
	@test nrow(df_removed) == 2
	@test df_removed[1, :OriginAirportID] == 14698
	@test df_removed[1, :DestAirportID]   == 14771
	@test df_removed[2, :OriginAirportID] == 14771 
	@test df_removed[2, :DestAirportID]   == 10713
end

@testset "clean_data_flights" begin
	weekday = 5
	df_cleaned_flights = clean_data_flights(df_flights, weekday, territoriesToDelte, df_airports)
	@test nrow(df_flights) == 7
	@test nrow(df_cleaned_flights)   == 2
	@test df_cleaned_flights[1, :OriginAirportID] == 14698
	@test df_cleaned_flights[1, :DestAirportID]   == 14771
	@test df_cleaned_flights[2, :OriginAirportID] == 14771 
	@test df_cleaned_flights[2, :DestAirportID]   == 10713
	@test df_cleaned_flights[1, :Tail_Number] == "tail1"
	@test df_cleaned_flights[2, :Tail_Number] == "tail4"
end

@testset "get_set_airports_in_flight_df" begin
	weekday = 5
	df_cleaned_flights = clean_data_flights(df_flights, weekday, territoriesToDelte, df_airports)
	airports = get_set_airports_in_flight_df(df_cleaned_flights)
	@test 10713 in airports
	@test 14771 in airports
	@test 14698 in airports
	# If the following test fails means that we have added flights to df_flights
	@test length(airports) == 3
end

@testset "remove_airports_not_in_flight_df_and_drop_some_columns" begin
	weekday = 5
	df_cleaned_flights = clean_data_flights(df_flights, weekday, territoriesToDelte, df_airports)
	airports = get_set_airports_in_flight_df(df_cleaned_flights)
	df_airports_after_remove = remove_airports_not_in_flight_df_and_drop_some_columns(df_airports, airports);
	@test ncol(df_airports_after_remove) == 3
	@test nrow(df_airports_after_remove) == 6
	airports_in_df = Set(df_airports_after_remove.AIRPORT_ID)
	@test length(airports_in_df) == 3
	@test 10713 in airports_in_df
	@test 14771 in airports_in_df
	@test 14698 in airports_in_df
end

@testset "clean_data_airports" begin
	weekday = 5
	df_cleaned_flights = clean_data_flights(df_flights, weekday, territoriesToDelte, df_airports)
	df_cleaned_airports = clean_data_airports(df_airports, df_cleaned_flights)
	@test ncol(df_cleaned_airports) == 3
	@test nrow(df_cleaned_airports) == 3
	airports_in_df = Set(df_cleaned_airports.AIRPORT_ID)
	@test 10713 in airports_in_df
	@test 14771 in airports_in_df
	@test 14698 in airports_in_df
end

@testset "transform_time_to_minutes" begin
	weekday = 5
	df_cleaned_flights = clean_data_flights(df_flights, weekday, territoriesToDelte, df_airports)
	transform_time_to_minutes!(df_cleaned_flights)
	@test df_cleaned_flights[1,:DepTime] == 13*60 + 53
	@test df_cleaned_flights[1,:ArrTime] == 14*60 + 44
	@test df_cleaned_flights[2,:DepTime] == 13*60 + 53
	@test df_cleaned_flights[2,:ArrTime] == 14*60 + 44
end

function add_fake_flights_to_test_connections!(df_flights::DataFrame)
	df_fake = DataFrame(Tail_Number = ["tail1", "tail1", "tail4", "tail4"],
						OriginCityName = ["XXX", "YYY", "Boise, ID", "ZZZ"],
						DestCityName = ["YYY", "VVV", "ZZZ", "DDD"],
						OriginAirportID = [14772, 14773, 10713, 10714],
						DestAirportID = [14773, 14774, 10714, 10715],
						DepTime = [1444, 1544, 1544, 1644],
						ArrTime = [1520, 1620, 1620, 1720]
						)
	append!(df_flights, df_fake)
end

@testset "get_missing_connections" begin
	weekday = 5
	df_cleaned_flights  = clean_data_flights(df_flights, weekday, territoriesToDelte, df_airports)
	add_fake_flights_to_test_connections!(df_cleaned_flights)
	missingConnections = get_missing_connections!(df_cleaned_flights)
	@test length(missingConnections) == 1
	@test missingConnections[1] == 2
end

@testset "repair_connection" begin
	weekday = 5
	df_cleaned_flights  = clean_data_flights(df_flights, weekday, territoriesToDelte, df_airports)
	add_fake_flights_to_test_connections!(df_cleaned_flights)
	missingConnections = get_missing_connections!(df_cleaned_flights)
	idxMissingFlight = 2
	idxNextMissingFlight = 6
	idx = 1
	repair_connection!(idx, idxMissingFlight, idxNextMissingFlight, df_cleaned_flights)
	@test df_cleaned_flights[1,:Tail_Number] == "tail1"
	@test df_cleaned_flights[2,:Tail_Number] == "aircraft1"
	@test df_cleaned_flights[3,:Tail_Number] == "aircraft1"
	@test df_cleaned_flights[4,:Tail_Number] == "tail4"
	@test df_cleaned_flights[5,:Tail_Number] == "tail4"
	@test df_cleaned_flights[6,:Tail_Number] == "tail4"
end

@testset "assign_new_tail_number_to_missing_connections" begin
	weekday = 5
	df_cleaned_flights  = clean_data_flights(df_flights, weekday, territoriesToDelte, df_airports)
	add_fake_flights_to_test_connections!(df_cleaned_flights)
	assign_new_tail_number_to_missing_connections!(df_cleaned_flights)
	@test df_cleaned_flights[1,:Tail_Number] == "tail1"
	@test df_cleaned_flights[2,:Tail_Number] == "aircraft1"
	@test df_cleaned_flights[3,:Tail_Number] == "aircraft1"
	@test df_cleaned_flights[4,:Tail_Number] == "tail4"
	@test df_cleaned_flights[5,:Tail_Number] == "tail4"
	@test df_cleaned_flights[6,:Tail_Number] == "tail4"
end

@testset "convert_tail_to_integer_and_add_index_of_previous_flight" begin
	weekday = 5
	df_cleaned_flights  = clean_data_flights(df_flights, weekday, territoriesToDelte, df_airports)
	add_fake_flights_to_test_connections!(df_cleaned_flights)
    transform_time_to_minutes!(df_cleaned_flights);
	assign_new_tail_number_to_missing_connections!(df_cleaned_flights)
    insertcols!(df_cleaned_flights, :flight => 1:nrow(df_cleaned_flights));
    insertcols!(df_cleaned_flights, :seq => -1);
    convert_tail_to_integer_and_add_index_of_previous_flight!(df_cleaned_flights);
	@test df_cleaned_flights[1,:Tail_Number] == 1
	@test df_cleaned_flights[2,:Tail_Number] == 2
	@test df_cleaned_flights[3,:Tail_Number] == 2
	@test df_cleaned_flights[4,:Tail_Number] == 3
	@test df_cleaned_flights[5,:Tail_Number] == 3
	@test df_cleaned_flights[6,:Tail_Number] == 3
	@test df_cleaned_flights[1,:seq] == -1
	@test df_cleaned_flights[2,:seq] == -1
	@test df_cleaned_flights[3,:seq] == 2
	@test df_cleaned_flights[4,:seq] == -1
	@test df_cleaned_flights[5,:seq] == 4
	@test df_cleaned_flights[6,:seq] == 5
	@test df_cleaned_flights[1,:flight] == 1
	@test df_cleaned_flights[2,:flight] == 2
	@test df_cleaned_flights[3,:flight] == 3
	@test df_cleaned_flights[4,:flight] == 4
	@test df_cleaned_flights[5,:flight] == 5
	@test df_cleaned_flights[6,:flight] == 6
end

@testset "correct_time_incoherences" begin
	weekday = 5
	df_cleaned_flights  = clean_data_flights(df_flights, weekday, territoriesToDelte, df_airports)
	add_fake_flights_to_test_connections!(df_cleaned_flights)
    transform_time_to_minutes!(df_cleaned_flights);
	assign_new_tail_number_to_missing_connections!(df_cleaned_flights)
    insertcols!(df_cleaned_flights, :flight => 1:nrow(df_cleaned_flights));
    insertcols!(df_cleaned_flights, :seq => -1);
    convert_tail_to_integer_and_add_index_of_previous_flight!(df_cleaned_flights);
	df_cleaned_flights[3,:DepTime] = 910 
	df_cleaned_flights[5,:DepTime] = 883
	correct_time_incoherences!(df_cleaned_flights)
	@test df_cleaned_flights[3,:DepTime] == 950 
	@test df_cleaned_flights[5,:DepTime] == 914
end

@testset "modify_data_flights" begin
	weekday = 5
	df_cleaned_flights  = clean_data_flights(df_flights, weekday, territoriesToDelte, df_airports)
	add_fake_flights_to_test_connections!(df_cleaned_flights)
	modify_data_flights!(df_cleaned_flights)
	@test df_cleaned_flights[1,:Tail_Number] == 1
	@test df_cleaned_flights[2,:Tail_Number] == 2
	@test df_cleaned_flights[3,:Tail_Number] == 2
	@test df_cleaned_flights[4,:Tail_Number] == 3
	@test df_cleaned_flights[5,:Tail_Number] == 3
	@test df_cleaned_flights[6,:Tail_Number] == 3
	@test df_cleaned_flights[1,:seq] == -1
	@test df_cleaned_flights[2,:seq] == -1
	@test df_cleaned_flights[3,:seq] == 2
	@test df_cleaned_flights[4,:seq] == -1
	@test df_cleaned_flights[5,:seq] == 4
	@test df_cleaned_flights[6,:seq] == 5
	@test df_cleaned_flights[1,:flight] == 1
	@test df_cleaned_flights[2,:flight] == 2
	@test df_cleaned_flights[3,:flight] == 3
	@test df_cleaned_flights[4,:flight] == 4
	@test df_cleaned_flights[5,:flight] == 5
	@test df_cleaned_flights[6,:flight] == 6
end
