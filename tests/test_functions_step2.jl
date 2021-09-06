
using Test
using Queryverse
const testdir = dirname(@__FILE__)
include(testdir*"/../functions_step1.jl")
include(testdir*"/../functions_step2.jl")

Base.@kwdef struct Input
    # Equivalence between minutes and periods
    I_periods::Int = 5; # 1 period = 5 minutes

    # SECTORS
    # number of columns and rows for the grid of the sectors
    I_numCol::Int = 20;
    I_numRow::Int = 20;

    # DEPARTURE & LANDING
    # Number of periods in departure and landing operations 
    I_periodDep::Int  = 1;
    I_periodLand::Int = 1;
    # Max departure delay. This is equal for all the flights.
    I_maxPeriodDelayDep::Int = ceil(Int, 1.5 * 60/I_periods); # 1:30 hours

    # FLYING
    # speed of the aircraft (km/h)
    D_speedAircraft::Float64 = 885;
    # % change of speed for delay and increase in air
    D_perDelay::Float64 = 0.25;
    D_perIncre::Float64 = 0.25;
    # Minimum s_{f,f'} time
    I_extraTime = 6;

    # ALTERNATIVES ROUTES
    # include alternative routes?
    B_altRoutes::Bool = true;
    # max number of alternative routes.
    I_maxNumRoutes::Int = 4;
    # percentage of sectors to check for alternative routes
    D_perSectors::Float64 = 0.05; # = 5%
    # cost to penalize arcs and obtain alternative routes (DO NOT modify this value)
    I_cost = 10_000;
end
input = Input();

df_flights = DataFrame(load(testdir*"/fakeFlightsForTests.csv"))
df_airports = DataFrame(load(testdir*"/fakeAirportsForTests.csv"))

territoriesToDelte = ["Alaska", "Hawaii"];
weekday = 5
df_cleaned_flights = clean_data_flights(df_flights, weekday, territoriesToDelte, df_airports)
df_cleaned_airports = clean_data_airports(df_airports, df_cleaned_flights)


@testset "get_extreme_points" begin
	xmin, xmax, ymin, ymax = get_extreme_points(df_cleaned_airports);
	@test xmin < xmax 
	@test xmax - xmin >= 2
	@test ymin < ymax 
	@test ymax - ymin >= 2
	@test xmin < -122
	@test xmax > -116
	@test ymin < 35
	@test ymax > 44
end

@testset "assign_sectors_to_airports" begin
	xmin, xmax, ymin, ymax = get_extreme_points(df_cleaned_airports);
	Δcol = (xmax - xmin)/input.I_numCol; 
  	Δrow = (ymax - ymin)/input.I_numRow; 
	assign_sectors_to_airports!(df_cleaned_airports, input, xmin, 
                                Δcol, ymax, Δrow)
	@test df_cleaned_airports[1, :sector] == 262
	@test df_cleaned_airports[2, :sector] == 366
	@test df_cleaned_airports[3, :sector] == 37
end