struct Point
    node::Int;
    x::Float64;
    y::Float64;
end

function create_sector_and_route_information!(input, DF_airports::DataFrame)
  	# Aiports
  	xmin, xmax, ymin, ymax = get_extreme_points(DF_airports);
  	Δcol = (xmax - xmin)/input.I_numCol; # size of each column in the grid
  	Δrow = (ymax - ymin)/input.I_numRow; # size of each rown in the grid
  	assign_sectors_to_airports!(DF_airports, input, xmin, Δcol, ymax, Δrow);

  	# Inner nodes
  	# Divide by 2 to consider the middle node in sectors. See in create_coordinates_inner_nodes()
  	Δcol = Δcol/2; # horizontal distance to reach the next node
  	Δrow = Δrow/2; # vertical distance to reach the next node
  	pointsMiddle = create_coordinates_inner_nodes(input, xmin, xmax, ymax, Δrow, Δcol);
 
  	# Graph of routes
  	DF_graph, dictAirportNode = create_graph_of_routes(input, DF_airports, 
	 												pointsMiddle, Δrow, Δcol);
  
  	return DF_graph, dictAirportNode;
end

function get_extreme_points(DF_airports::DataFrame)
  	xmin = minimum(DF_airports[!, :LONGITUDE]);
  	xmax = maximum(DF_airports[!, :LONGITUDE]);
  	ymin = minimum(DF_airports[!, :LATITUDE]);
  	ymax = maximum(DF_airports[!, :LATITUDE]);

  	# Increase extreme points so the resulting square
  	# does not have airports at the edges
  	xmin -= 1; xmax += 1;
  	ymin -= 1; ymax += 1; 

  	return xmin, xmax, ymin, ymax;
end

function assign_sectors_to_airports!(DF_airports::DataFrame, 
                                     input, 
                                     xmin::Float64, 
                                     Δcol::Float64, 
                                     ymax::Float64, 
                                     Δrow::Float64)
  	posNewColumn = ncol(DF_airports) + 1;
  	# The map is a grid, so each airport will be assigned as a sector the number of
  	# the grid where it is. Sector 0 = (xmin, ymax). Sector 1 = (xmin+Δcol, ymax). Thus:
  	sectorOfEachAirport = floor.(Int, (DF_airports[!, :LONGITUDE] .- xmin)/Δcol) +
  	   		input.I_numCol*(ceil.(Int, (ymax .- DF_airports[!, :LATITUDE])/Δrow) .- 1);

  	insertcols!(DF_airports, posNewColumn, sector = sectorOfEachAirport);
  	return nothing;
end

function create_coordinates_inner_nodes(input, 
                                        xmin::Float64, 
                                        xmax::Float64, 
                                        ymax::Float64, 
                                        Δrow::Float64, 
                                        Δcol::Float64)
  	# The frame formed by (xmin, ymin), (xmin, ymax), (xmax, ymin), (xmax, ymax)
  	# will be divided into sectors of size: Δcol × Δrow.
  	# For each sector, we will have 9 points: 4 in the vertexes, 4 in the edges and
  	# 1 in the middle. This part of the code is for the latter. 
  	# Check the next Fig (representing 1 sector), where '*' are the points:
  
  	#   *----*----*
  	#   |---------|
  	#   *----*----* # we seek the coordinate for the middle point in each sector!
  	#   |---------|
  	#   *----*----*
  
  	# Notice that the 8 outer nodes connect the sector with the adjacency sectors,
  	# while the inner node is to simulate more realistic routes inside the sector.
  	# Inside a sector, the outer nodes are only connected with the inner node. 
	# Thus no route in the edges/boundaries of a sector is created.
  
	# NOTE: check tests for an alternative way for this function (maybe it is easier to understand there)
  	nPointsCol    = 2*input.I_numCol + 1; # number of points in any row
  	nMiddlePoints = input.I_numCol*input.I_numRow; # number of middle points (1 per sector)
  	pointsMiddle  = Array{Point, 1}(undef, nMiddlePoints); # vector to fill with the middle points

  	Δ      = 2*nPointsCol;     # if node K is in pos (i,j), then K + Δ is in (i+1, j)
  	point0 = 2 + nPointsCol;   # node number of the first middle (inner) node
  	pointF = 2*nPointsCol - 1; # node number of the last middle (inner) node in the same row that point0
  	valsX  = collect((xmin + Δcol):2*Δcol:xmax); # x-coordinate of the middle nodes for any row
  
  	# Create all the middle (inner) points
  	for n in 1:input.I_numRow
  	    range  = ((n-1)*input.I_numCol + 1):(n*input.I_numCol);
  	    valsN  = (point0 + (n-1)*Δ):2:(pointF + (n-1)*Δ); # nodes number (i.e, name)
  	    valsY  = repeat([ymax - Δrow - 2*Δrow*(n-1)], inner = input.I_numCol); # y-coordinate
  	    pointsMiddle[range] = Point.(collect(valsN), valsX, valsY);
  	end
  	return pointsMiddle;
end

function create_graph_of_routes(input, 
                                DF_airports::DataFrame, 
                                pointsMiddle::Array{Point, 1}, 
                                Δrow::Float64, 
                                Δcol::Float64)
  	# We connect the middle inner nodes, with all the outer nodes
  	# that are in the SAME sector. That is, we are going to obtain the following 8 arcs:
  	#   a    b    c
  	#     \  |   /  # 3 arcs: (e,a), (e,b) and (e,c)
  	#   d----e----f # 2 arcs: (e,d) and (e,f)
  	#     /  |   \  # 3 arcs: (e,g), (e,h) and (e,i)
  	#   g    h    i
 
  	nMiddlePoints = input.I_numCol*input.I_numRow; # number of middle points (1 per sector)
  	DF_graph = create_empty_graph(nMiddlePoints);
  	dictSectorPoints = fill_df_graph_with_connections!(input, DF_graph, 
	 													pointsMiddle, Δrow, Δcol);
  	dictAirportNode = fill_df_graph_with_airports_connections!(dictSectorPoints, 
	 														DF_graph, DF_airports);

  	return DF_graph, dictAirportNode;
end

function create_empty_graph(nMiddlePoints::Int)
  	DF_graph = DataFrame(nArc   = zeros(Int, 8*nMiddlePoints), # number of the arc
  	                     tail   = zeros(Int, 8*nMiddlePoints),
  	                     head   = zeros(Int, 8*nMiddlePoints),
  	                     cost   = zeros(Float64, 8*nMiddlePoints),
  	                     sector = repeat(["Sector"], inner = 8*nMiddlePoints)
  	                     );
  	return DF_graph;
end

function fill_df_graph_with_connections!(input, 
                                         DF_graph::DataFrame, 
                                         pointsMiddle::Array{Point,1}, 
                                         Δrow::Float64, 
                                         Δcol::Float64)

  	# We will need the following Dict when adding the airports' connections.
  	# It will contain the outer nodes of each sector
  	dictSectorPoints = Dict{Int, Array{Point, 1}}();
  	local R = 6371; # km of Earth radius: for haversine distance
  
  	# Distances between points (recall figure in create_graph_of_routes()):
  	costΔrow = haversine([0, 0], [0, Δrow], R); # vertical distance (e.g, in arc (e,h))
  	costΔcol = haversine([0, 0], [Δcol, 0], R); # horizontal (e.g, in arc (e,f))
  	D_dist   = haversine([0, 0], [Δcol, Δrow], R); # diagonal (e.g, in arc (e,i))
  
  	# Connection of points 
  	nPointsCol = 2*input.I_numCol + 1; # number of points in any row
  	idxArc = 0; 
  	idxSector = 0; 
  	for p in pointsMiddle
  	    sector = "S" * string(idxSector);
  	    vectPoints = Array{Point, 1}(); # to feed the dictionary
  	    # Arcs (e,b) and (e,h)
  	    DF_graph[idxArc + 1, :] .= (idxArc, p.node, p.node - nPointsCol, costΔrow, sector);
  	    push!(vectPoints, Point(p.node - nPointsCol, p.x, p.y + Δrow)); # '+' because I go up
  	    DF_graph[idxArc + 2, :] .= (idxArc + 1, p.node, p.node + nPointsCol, costΔrow, sector);
  	    push!(vectPoints, Point(p.node + nPointsCol, p.x, p.y - Δrow)); # '-' because I go down
  
  	    # Arcs (e,c), (e,i), (e,f)
  	    DF_graph[idxArc + 3, :] .= (idxArc + 2, p.node, p.node - nPointsCol + 1, D_dist, sector);
  	    push!(vectPoints, Point(p.node - nPointsCol + 1, p.x + Δcol, p.y + Δrow));
  	    DF_graph[idxArc + 4, :] .= (idxArc + 3, p.node, p.node + nPointsCol + 1, D_dist, sector);
  	    push!(vectPoints, Point(p.node + nPointsCol + 1, p.x + Δcol, p.y - Δrow));
  	    DF_graph[idxArc + 5, :] .= (idxArc + 4, p.node, p.node + 1, costΔcol, sector);
  	    push!(vectPoints, Point(p.node + 1, p.x + Δcol, p.y));
  
  	    # Arcs (e,a), (e,g), (e,d)
  	    DF_graph[idxArc + 6, :] .= (idxArc + 5, p.node, p.node - nPointsCol - 1, D_dist, sector);
  	    push!(vectPoints, Point(p.node - nPointsCol - 1, p.x - Δcol, p.y + Δrow));
  	    DF_graph[idxArc + 7, :] .= (idxArc + 6, p.node, p.node + nPointsCol - 1, D_dist, sector);
  	    push!(vectPoints, Point(p.node + nPointsCol - 1, p.x - Δcol, p.y - Δrow));
  	    DF_graph[idxArc + 8, :] .= (idxArc + 7, p.node, p.node - 1, costΔcol, sector);
  	    push!(vectPoints, Point(p.node - 1, p.x - Δcol, p.y));
  
  	    dictSectorPoints[idxSector] = vectPoints;
  	    idxArc  += 8;
  	    idxSector += 1;
  	end

  	return dictSectorPoints;
end

function fill_df_graph_with_airports_connections!(dictSectorPoints::Dict{Int, Array{Point, 1}}, 
                                                  DF_graph::DataFrame, 
                                                  DF_airports::DataFrame)
  	local R = 6371; # km of Earth radius: for haversine distance

  	dictAirportNode = Dict{Int, Int}(); # number of node assigned to each airport
  	I_counter2 = maximum(DF_graph[!, :head]) + 1; # max node cannot be in the :tail
  	I_counter  = nrow(DF_graph);
  
  	# Connecting each aiport with the 8 outer nodes of its sector
  	for r in eachrow(DF_airports)
  	    # get the sector of the airport
  	    sector = "S" * string(r[:sector]);
  	    vectPoints = dictSectorPoints[r[:sector]];
  	    for p in vectPoints
  	        D_dist = haversine([p.x, p.y], [r[:LONGITUDE], r[:LATITUDE]], R);
  	        push!(DF_graph, (I_counter, I_counter2, p.node, D_dist, sector));
  	        I_counter += 1;
  	    end
  	    dictAirportNode[r[:AIRPORT_ID]] = I_counter2;
  	    I_counter2 += 1;
  	end

  	return dictAirportNode;
end

