function clean_data_flights(df_flights_raw::DataFrame, 
                            weekday::Int, 
                            territoriesToDelete::Array{String,1}, 
                            df_airports_raw::DataFrame)::DataFrame

	df_contiguous = remove_flights_in_non_contiguous_USA(territoriesToDelete, df_flights_raw);
	df_filtered = filter_observation_and_keep_only_useful_columns(df_contiguous, weekday);
	df_final = remove_flights_with_no_airport_in_the_data_set(df_airports_raw, df_filtered);
    return df_final;
end

function remove_flights_in_non_contiguous_USA(territoriesToDelte::Array{String,1}, 
											  df_flights_raw::DataFrame)
	contiguousUSOrigin = map(x -> x ∉ territoriesToDelte, df_flights_raw.OriginStateName);
	contiguousUSDest   = map(x -> x ∉ territoriesToDelte, df_flights_raw.DestStateName);
    contiguousUS = contiguousUSOrigin .& contiguousUSDest;
    df_flights_raw = df_flights_raw[contiguousUS, :];
	return df_flights_raw
end

function filter_observation_and_keep_only_useful_columns(df_flights_raw::DataFrame,
														 weekday::Int)
    df = df_flights_raw |> 
            @filter(_.DayofMonth == weekday) |>
            @filter(_.Cancelled  == 0.0 && _.Diverted == 0.0) |>
            @select(:Tail_Number, 
                    :OriginCityName, :DestCityName,
                    :OriginAirportID, :DestAirportID,
                    :DepTime, :ArrTime 
                    ) |>
            @dropna() |>
            DataFrame;
	return df
end

function remove_flights_with_no_airport_in_the_data_set(df_airports_raw::DataFrame,
														df_flights::DataFrame)
    airports = BitSet();
    sizehint!(airports, nrow(df_airports_raw));
    for r in eachrow(df_airports_raw)
        push!(airports, r[:AIRPORT_ID]);
    end
    df = df_flights |> 
        @filter(_.OriginAirportID in airports && 
                _.DestAirportID   in airports) |> 
        DataFrame;
	return df
end

function clean_data_airports(df_airports_raw::DataFrame, df_flights::DataFrame)
	airports = get_set_airports_in_flight_df(df_flights);
	df = remove_airports_not_in_flight_df_and_drop_some_columns(df_airports_raw, airports);
    unique!(df, [:AIRPORT_ID]); # drop duplicates
    return df;
end

function get_set_airports_in_flight_df(df_flights::DataFrame)
    airports = BitSet();
    for r in eachrow(df_flights)
        push!(airports, r[:OriginAirportID]);
        push!(airports, r[:DestAirportID]);
    end
	return airports
end

function remove_airports_not_in_flight_df_and_drop_some_columns(df_airports_raw::DataFrame, 
										  						airports::BitSet)
    df = df_airports_raw |>
        @filter(_.AIRPORT_ID in airports) |>
        @select(:AIRPORT_ID, :LATITUDE, :LONGITUDE) |>
        DataFrame;
	return df
end


function modify_data_flights!(df_flights::DataFrame)
    transform_time_to_minutes!(df_flights);
    assign_new_tail_number_to_missing_connections!(df_flights);
    
    insertcols!(df_flights, :flight => 1:nrow(df_flights));
    insertcols!(df_flights, :seq => -1);# seq will contain the flight index of the previous flight (if any)

    convert_tail_to_integer_and_add_index_of_previous_flight!(df_flights);
    correct_time_incoherences!(df_flights);
end

function transform_time_to_minutes!(df_flights::DataFrame)
	if typeof(df_flights[1,:DepTime]) == String
    	df_flights.DepTime = parse.(Int, df_flights.DepTime);
    	df_flights.ArrTime = parse.(Int, df_flights.ArrTime);
	end
    df_flights.DepTime = 60 * div.(df_flights.DepTime, 100) + df_flights.DepTime .% 100;
    df_flights.ArrTime = 60 * div.(df_flights.ArrTime, 100) + df_flights.ArrTime .% 100;
end

function assign_new_tail_number_to_missing_connections!(df_flights::DataFrame)
	idxMissingConnections = get_missing_connections!(df_flights)
	nFlights = nrow(df_flights)
    for i in 1:length(idxMissingConnections)
 	    idxMissingFlight = idxMissingConnections[i];
 	    idxNextMissingFlight = get_index_next_missing_flight(i, nFlights, idxMissingConnections)
		repair_connection!(i, idxMissingFlight, idxNextMissingFlight, df_flights)
    end
end

function repair_connection!(i::Int, 
							idxMissingFlight::Int, 
							idxNextMissingFlight::Int, 
							df_flights::DataFrame)
 	tail = df_flights[idxMissingFlight, :Tail_Number];
	idx = idxMissingFlight
	flights_belong_to_the_same_aircraft = df_flights[idx, :Tail_Number] == tail
	next_flight_is_not_the_next_missing_one = idx < idxNextMissingFlight
    while flights_belong_to_the_same_aircraft && next_flight_is_not_the_next_missing_one
		df_flights[idx, :Tail_Number] = "aircraft" * string(i);
		idx += 1;
		flights_belong_to_the_same_aircraft = df_flights[idx, :Tail_Number] == tail
		next_flight_is_not_the_next_missing_one = idx < idxNextMissingFlight
    end
end

function get_index_next_missing_flight(i::Int, 
									   nFlights::Int, 
									   idxMissingConnections::Array{Int,1})
	if i < length(idxMissingConnections) 
		return idxMissingConnections[i+1] 
	else 
		return nFlights;
	end
end

function get_missing_connections!(df_flights::DataFrame)
    missingConnections = Int[];
    sort!(df_flights, [:Tail_Number, :DepTime]);
    aircraft = df_flights[1, :Tail_Number];
    for idxFlight in 2:nrow(df_flights)
        if is_the_flight_operated_by_the_current_aircraft(aircraft, idxFlight, df_flights)
            if is_the_flight_connected_to_its_predecessor(idxFlight, df_flights) == false
                push!(missingConnections, idxFlight);
            end
        else
            aircraft = df_flights[idxFlight, :Tail_Number];
        end
    end
	return missingConnections
end

function is_the_flight_operated_by_the_current_aircraft(tail::String, 
													    currentFlight::Int, 
													    df_flights::DataFrame)
	return tail == df_flights[currentFlight, :Tail_Number]
end

function is_the_flight_connected_to_its_predecessor(idxCurrentFlight::Int, 
													df_flights::DataFrame)
	depAirportCurrentFlight  = df_flights[idxCurrentFlight, :OriginAirportID] 
	arrAirportPreviousFlight = df_flights[idxCurrentFlight-1, :DestAirportID]
	return depAirportCurrentFlight == arrAirportPreviousFlight
end

function convert_tail_to_integer_and_add_index_of_previous_flight!(df_flights::DataFrame)
    tailNum  = unique(df_flights.Tail_Number);
    aircraft = Dict(tailNum[i] => string(i) for i = 1:length(tailNum));
    tail = df_flights[1, :Tail_Number];
    df_flights[1, :Tail_Number] = aircraft[tail];
    for i in 2:nrow(df_flights)
        if tail != df_flights[i, :Tail_Number]
            tail = df_flights[i, :Tail_Number];
        else
            df_flights[i, :seq] = i-1;
        end
        df_flights[i, :Tail_Number] = aircraft[tail];
    end
    df_flights.Tail_Number = parse.(Int, df_flights.Tail_Number);
end

function correct_time_incoherences!(df_flights::DataFrame)
  for i in 2:nrow(df_flights)
    if df_flights[i, :seq] != -1
      if df_flights[i, :DepTime] <= df_flights[i-1, :ArrTime]
        df_flights[i, :DepTime] = df_flights[i-1, :ArrTime] + 30; # 30 min of slack
      end
    end
  end
end

"""
	test_df_flights

This method tests that the final data frame containing the flights
does not have obvious mistakes
"""
function test_df_flights(df_flights::DataFrame)
  for i in 2:nrow(df_flights)
    if df_flights[i, :seq] == -1
      if df_flights[i, :Tail_Number] == df_flights[i-1, :Tail_Number]
		# seq = -1 implies first flight of the sequence. However, the previos flight
		# has the same tail number => ERROR
        println("Error in seq = -1 for row ", i);
      end
    else  # The flight is a continued one
      if df_flights[i, :seq] != df_flights[i-1, :flight]
		# The the predecessor flight info in seq does not match with the flight info
		# of the previos flight
        println("Error in seq != flight in row", i);
      end
      if df_flights[i, :OriginAirportID] != df_flights[i-1, :DestAirportID]
		# The origin airport does not match with the destination airport of 
		# the predecessor flight
        println("Error in origin != destination in row", i);
      end
      if df_flights[i, :DepTime] <= df_flights[i-1, :ArrTime]
		# The departure time is earlier than the arrival time of the predecessor flight
        println("Error: dep Time <= arr time  in row", i, " ", df_flights[i, :DepTime], " ", df_flights[i-1, :ArrTime]);
      end
    end
  end
end
