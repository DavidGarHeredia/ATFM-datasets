#-----------------------------------
# Main functions used in the code
#-----------------------------------

function create_main_3droutes(DF_flights::DataFrame, 
                              network::MetaGraph{Int64,Float64},
                              dictAirportNode::Dict{Int64,Int64},
                              input::Input, 
                              dictAirportDummies::Dict{Int, Array{Int, 1}})
    DF_3d = return_emptyDF_3dRoutes();
    for f in eachrow(DF_flights)
        route = 1; # 1 because is the main
        DF_aux = generate_route_for_flight(f, network, dictAirportNode, 
                                           input, dictAirportDummies, route);
        append!(DF_3d, DF_aux);
    end
    return DF_3d;
end

# Generates the route and time information for the shortest path
# connecting the departure and landing airports of flight f.
function generate_route_for_flight(f::DataFrameRow{DataFrame,DataFrames.Index},
                                   network::MetaGraph{Int64,Float64},
                                   dictAirportNode::Dict{Int64,Int64},
                                   input::Input, 
                                   dictAirportDummies::Dict{Int, Array{Int, 1}},
                                   route::Int)
    DF_aux = create_route_based_on_shortest_path(f, network, dictAirportNode, route);
    add_time_info!(DF_aux, f[:DepTime], input); 
    add_arc_for_dep_and_land!(DF_aux, input, dictAirportDummies, route);
    return DF_aux;
end

# Check if, given the continued flights f -> f', it is true that:
# Δt = departure time f' - land time of f >= turnaround time. Fix it if not.
function check_time_in_continued_flights!(DF_3droutes::DataFrame, input::Input)
    notFixed, flightsToFix, Δt = checking_flights(DF_3droutes, input);
    while notFixed
        fix_time!(DF_3droutes, flightsToFix, Δt, input);
        notFixed, flightsToFix, Δt = checking_flights(DF_3droutes, input);
    end
end

# Check if the minimum s_{f,f'} (turnaround time) is respected
# Note: Δt[f] = depTime[f'] - landTime[f] 
function checking_flights(DF_3droutes::DataFrame, input::Input)
    Δt = zeros(Int, maximum(DF_3droutes.flight));
    for r in eachrow(DF_3droutes)
        if r[:phase] == "dep" && r[:prevFlight] != -1 && r[:route] == 1
            Δt[r[:prevFlight]] = r[:bt];
        end
    end
    numberOfFlightsWithNoPredecessor = sum(Δt .== 0); 
    flightsWithNoPredecessor = findall(Δt .== 0);

    for r in eachrow(DF_3droutes)
        if r[:phase] == "land" && r[:route] == 1
            Δt[r[:flight]] -= r[:et];
        end
    end
    numberOfFlightsViolatingCondition = sum(Δt .< input.I_extraTime);

    if numberOfFlightsViolatingCondition > numberOfFlightsWithNoPredecessor 
        flightsViolatingCondition = findall(Δt .< input.I_extraTime);
        # +1 because we want the flight that departs
        flightsToFix = setdiff(flightsViolatingCondition, flightsWithNoPredecessor) .+ 1; 
        return true, flightsToFix, Δt;
    else
        return false, zeros(Int, 1), zeros(Int, 1);
    end
end

# Correcting the time of the flight to guarantee a minimum s_{f,f'}
function fix_time!(DF_3droutes::DataFrame, 
                   flightsToFix::Array{Int64,1},
                   Δt::Array{Int64,1}, 
                   input::Input)
    for f in flightsToFix
        timeIncreaseRequired = input.I_extraTime - Δt[f - 1];
        DF_3droutes[DF_3droutes.flight .== f, :bt] .+= timeIncreaseRequired;
        DF_3droutes[DF_3droutes.flight .== f, :et] .+= timeIncreaseRequired;
    end
end











#-----------------------------------
# functions for the alternative routes
#-----------------------------------

function create_alternative_3droutes(DF_flights::DataFrame,
                                     network::MetaGraph{Int64,Float64},
                                     dictAirportNode::Dict{Int64,Int64},
                                     input::Input,
                                     dictAirportDummies::Dict{Int, Array{Int, 1}},
                                     dictSectArcs,
                                     dictFlightSect::Dict{Int64,Array{String,1}})

    DF_3d = return_emptyDF_3dRoutes();

    for (flight, sectorsUsed) in dictFlightSect
        increase_all_travel_times!(network, sectorsUsed, input.I_cost, dictSectArcs);
        nRoutesOfTheFlight = 1;
        f = DF_flights[flight, :];
        for sect in reverse(sectorsUsed) # reverse because we depenalize from end to start
            if nRoutesOfTheFlight <= input.I_maxNumRoutes
                DF_aux = generate_route_for_flight(f, network, dictAirportNode, input,
                                                   dictAirportDummies, nRoutesOfTheFlight + 1);
                append!(DF_3d, DF_aux);
            end
            # depenalize
            increase_travel_time!(network, sect, -input.I_cost, dictSectArcs);
            nRoutesOfTheFlight += 1;
        end
    end

    return DF_3d;
end

function get_K_more_used_sectors(DF_3droutes::DataFrame, input::Input,
                                network::MetaGraph{Int64,Float64},
                                dictAirportNode::Dict{Int64,Int64})

    # Usage of sectors without considering the airports
    nodeIsAirport = Dict(n => true for n in values(dictAirportNode));
    tailNodesThatAreNotAirports = findall(x -> !haskey(nodeIsAirport, x), DF_3droutes.tail);
    headNodesThatAreNotAirports = findall(x -> !haskey(nodeIsAirport, x), DF_3droutes.head);
    arcsWhereNoAirportsAppear   = intersect(tailNodesThatAreNotAirports, headNodesThatAreNotAirports);

    # Select the K most used sectors
    tbl = freqtable(DF_3droutes[arcsWhereNoAirportsAppear, :sector]);
    sort!(tbl, rev = true); # the more employed at the beginning
    K = ceil(Int, length(tbl) * input.D_perSectors);
    sectNames =  collect(keys(tbl.dicts[1]))[1:K]; # names of the sectors
    dictSectArcs = Dict(s => NamedTuple{(:tail, :head),
                                        Tuple{Int64,Int64}}[] for s in sectNames);

    # save the arcs that belong to those K sectors
    for e in edges(network)
      sectorN = get_prop(network, e.src, e.dst, :sector);
      if haskey(dictSectArcs, sectorN)
          push!(dictSectArcs[sectorN], (tail = e.src, head = e.dst));
      end
    end

    return dictSectArcs;
end

function get_flights_using_busy_sectors(DF_3droutes::DataFrame, dictSectArcs,
                                        dictAirportNode::Dict{Int64,Int64})

    # get flights that used at least one of the busy sectors
    arcsUsingBusySectors = findall(x -> haskey(dictSectArcs, x), DF_3droutes.sector);
    flightsUsingBusySectors = unique(DF_3droutes[arcsUsingBusySectors, :flight]);
    dictFlightSect = Dict(f => String[] for f in flightsUsingBusySectors);

    nodeIsAirport = Dict(n => true for n in values(dictAirportNode));

    # for each flight, we save the busy sectors that it uses
    for f in eachrow(DF_3droutes[arcsUsingBusySectors, [:flight, :sector, :tail, :head]])
        # if tail or head == airport, skip because it is worthless to
        # penalize that sector... it has to be used anyway
        if haskey(nodeIsAirport, f[:tail]) || haskey(nodeIsAirport, f[:head])
            # don't consider for flight 'f' that sector
        else
            push!(dictFlightSect[f[:flight]], f[:sector]);
        end
    end

    # delete empty flights
    for (key, value) in dictFlightSect
        if isempty(value)
            delete!(dictFlightSect, key);
        end
    end

    return dictFlightSect;
end

function increase_travel_time!(network::MetaGraph{Int64,Float64}, 
                               sector::String,
                               cost::Int, 
                               dictSectArcs)
    arcs = dictSectArcs[sector];
    for a in arcs
        currentCost = get_prop(network, a.tail, a.head, :weight);
        set_prop!(network, a.tail, a.head, :weight, cost + currentCost);
    end
end

function increase_all_travel_times!(network::MetaGraph{Int64,Float64},
                                    sectors::Array{String,1}, 
                                    cost::Int, 
                                    dictSectArcs)
    for s in sectors
        increase_travel_time!(network, s, cost, dictSectArcs);
    end
end




#-----------------------------------
# Secondary functions used in the code
#-----------------------------------

# Empty df to be filled with push!
function return_emptyDF_3dRoutes()
    df =  DataFrame(net    = Int[],
                    flight = Int[],
                    seq    = Int[],
                    tail   = Int[],
                    head   = Int[],
                    bt     = Int[],
                    et     = Int[],
                    delay  = Int[],
                    increase = Int[],
                    route = Int[],
                    prevFlight = Int[],
                    phase = String[],
                    sector = String[],
                    cost = Int[]
                );

    return df
end

# Empty df, but with the memory (number of rows) reserved. Thus, not need of push!
function return_filledDF_3dRoutes(tam::Int)
    df =  DataFrame(net    = zeros(Int, tam),
                    flight = zeros(Int, tam),
                    seq    = zeros(Int, tam),
                    tail   = zeros(Int, tam),
                    head   = zeros(Int, tam),
                    bt     = zeros(Int, tam),
                    et     = zeros(Int, tam),
                    delay  = zeros(Int, tam),
                    increase = zeros(Int, tam),
                    route = repeat([1], inner = tam),
                    prevFlight = zeros(Int, tam),
                    phase  = repeat(["S"], inner = tam),
                    sector = repeat(["S"], inner = tam),
                    cost   = zeros(Int, tam)
                );

    return df
end

# Returns a df row with all the information filled except for the time dimension
function fill_3dRoute_spatial(f::DataFrameRow{DataFrame,DataFrames.Index},
                            tail::Int, head::Int, route::Int,
                            phase::String, sector::String, seq::Int, cost::Int)
    df =  DataFrame(net    = f[:Tail_Number],
                    flight = f[:flight],
                    seq    = seq,
                    tail   = tail,
                    head   = head,
                    bt     = 0,
                    et     = 0,
                    delay  = 0,
                    increase = 0,
                    mainRoute = route,
                    prevFlight = f[:seq],
                    phase  = phase,
                    sector = sector,
                    cost = cost;
                );

    return collect(df[1, :]);
end

# Given a DF that contains the sequence of points that the aircraft has to
# traverse (e.g, a -> b -> c -> d), this function sets the arrival time to each
# node by adding the beginning time (bt) to the traversal time. Info about
# delays/increases is also added.
function add_time_info!(DF_aux::DataFrame, t0, input::Input)
    bt = t0;
    for r in eachrow(DF_aux)
        r[:bt] = bt;
        bt += r[:cost]; # end time and bt time of next iter
        r[:et] = bt;

        delayAllowed = input.D_perDelay * r[:cost];
        if delayAllowed - floor(delayAllowed) >= 0.75 # to round up with 0.75 instead of 0.5
            r[:delay] = round(Int, delayAllowed, RoundUp);
        else
            r[:delay] = round(Int, delayAllowed, RoundDown);
        end

        increaseAllowed = input.D_perIncre * r[:cost];
        if increaseAllowed - floor(increaseAllowed) >= 0.75 
            r[:increase] = round(Int, increaseAllowed, RoundUp);
        else
            r[:increase] = round(Int, increaseAllowed, RoundDown);
        end
    end
end

# Finds the shortest route between the departure and arrival airport. Then,
# saves the route info (nodes, sectors, traversal time of arcs...).
function create_route_based_on_shortest_path(f::DataFrameRow{DataFrame,DataFrames.Index},
                                             network::MetaGraph{Int64,Float64},
                                             dictAirportNode::Dict{Int64,Int64}, 
                                             nRoute::Int)
    # get the departure and destination airports
    origin = dictAirportNode[f[:OriginAirportID]];
    destination = dictAirportNode[f[:DestAirportID]];

    # compute Shortest Path
    route = enumerate_paths(dijkstra_shortest_paths(network, origin, weights(network)),
                            destination);
    numArcs = length(route) - 1;

    # create DF 3d routes and save info
    DF_aux = return_filledDF_3dRoutes(numArcs);
    for i in 1:numArcs
        tailN = route[i];
        headN = route[i + 1];
        sector = get_prop(network, tailN, headN, :sector);
        Δt = get_prop(network, tailN, headN, :weight);
        DF_aux[i, :] .= fill_3dRoute_spatial(f, tailN, headN, nRoute, "air",
                                                sector, i + 1, Δt);
    end

    # Traversing a sector (except for departures and landings) occurs as follows:
    # 1) The flight enters the sectors, 2) The flight goes to the middle
    # point, and 3) The flight exits the sector. Thus, the df will have, for those
    # sectors, two arcs with the same sector. We merge them in one with longer arc
    merge_arcs_with_duplicated_sectors!(DF_aux);

    return DF_aux;
end

function merge_arcs_with_duplicated_sectors!(DF_aux::DataFrame)
    n = nrow(DF_aux) - 1;
    posToDelete = Array{Int, 1}();

    i = 1;
    while i <= n
        timeForArcAfterMerge = DF_aux[i, :cost];
        numberOfArcsMerged = 0;
        # save info for the merge
        while DF_aux[i, :sector] == DF_aux[i + 1, :sector]
            i += 1;
            numberOfArcsMerged += 1;
            timeForArcAfterMerge  += DF_aux[i, :cost];
            push!(posToDelete, i);
            if i + 1 > n
                break;
            end
        end

        DF_aux[i - numberOfArcsMerged, :head] = DF_aux[i, :head]; # fix head
        DF_aux[i - numberOfArcsMerged, :cost] = timeForArcAfterMerge;
        i += 1;
    end
    deleterows!(DF_aux, posToDelete);

    # Adding seq info here. This has nothing to do with the merge
    DF_aux[!, :seq] .= collect(2:(1 + nrow(DF_aux))); 
end

# Departure and landing operations are handled as arcs in arc-based
# formulations. Here we add those arcs.
function add_arc_for_dep_and_land!(DF_aux::DataFrame, 
                                   input::Input,
                                   dictAirportDummies::Dict{Int, Array{Int, 1}}, 
                                   route::Int)

    DF_aux2 = return_filledDF_3dRoutes(2);
    # departure
    DF_aux2[1, :] .= (DF_aux[1, :net],
                    DF_aux[1, :flight],
                    DF_aux[1, :seq] - 1,
                    dictAirportDummies[DF_aux[1, :tail]][1], # dummyNode
                    DF_aux[1, :tail],
                    DF_aux[1, :bt] - input.I_periodDep,
                    DF_aux[1, :bt],
                    input.I_maxPeriodDelayDep,
                    0, # input.I_maxPeriodIncreDep
                    route,
                    DF_aux[1, :prevFlight],
                    "dep",
                    "A" * string(DF_aux[1, :tail]),
                    input.I_periodDep);

    # landing
    lastP = nrow(DF_aux);

    DF_aux2[2, :] .= (DF_aux[lastP, :net],
                    DF_aux[lastP, :flight],
                    DF_aux[lastP, :seq] + 1,
                    DF_aux[lastP, :head],
                    dictAirportDummies[DF_aux[lastP, :head]][2], # dummyNode
                    DF_aux[lastP, :et],
                    DF_aux[lastP, :et] + input.I_periodLand,
                    0,
                    0,
                    route,
                    DF_aux[lastP, :prevFlight],
                    "land",
                    "A" * string(DF_aux[lastP, :head]),
                    input.I_periodLand);

    # put info together
    append!(DF_aux, DF_aux2);
    sort!(DF_aux, :seq);
end

