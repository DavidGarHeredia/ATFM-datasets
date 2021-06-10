
function create_rhs(DF_3droutes::DataFrame, parameters::Parameters)
  matrixSectTime, dictPhaseSectorPosition = create_base_scenario(DF_3droutes, parameters);
  dictSectorAirports, dictSectorSectors = get_relationships_for_penalizing(DF_3droutes)
  # penlize_base_scenario!(matrixSectTime); # Habra parametros 3 base: none, medium, difficult
  # penalize_simulating_bad_weather!(matrixSectTime); # (blo) Habra parametros!!!
  # Dont forget to penalize airports (dep and arr) and join capacity constraints
  # TODO: garantizar una cap min de 1
  # DF_rhs = transform_matrix_to_data_frame();
  return DF_rhs;
end

is_an_airport(sector::String)= sector[1] == 'A';

# TODO: test this function
function get_relationships_for_penalizing(DF_3droutes::DataFrame)
  dictSectorAirports = Dict{String, Array{String}}();
  dictSectorSectors  = Dict{String, Array{String}}();
  N = nrow(DF_3droutes)-1
  for i in 2:N
    sector = DF_3droutes[i,:sector];
    if !is_an_airport(sector)
      next_sector = DF_3droutes[i+1,:sector];
      if is_an_airport(next_sector)
        push!(dictSectorAirports[sector], next_sector);
      else
        push!(dictSectorSectors[sector], next_sector);
      end
      prev_sector = DF_3droutes[i-1,:sector];
      if is_an_airport(prev_sector)
        push!(dictSectorAirports[sector], prev_sector);
      else
        push!(dictSectorSectors[sector], prev_sector);
      end
  end
end

function create_base_scenario(DF_3droutes::DataFrame, parameters::Parameters)
  matrixSectTime, dictPhaseSectorPosition = create_matrix_of_sector_usage(DF_3droutes);
  assign_base_values!(matrixSectTime, dictPhaseSectorPosition, Val(parameters.baseValuesRule));
  matrixJoin, dictJoin = get_join_capacity_constraints(matrixSectTime, 
                            dictPhaseSectorPosition, parameters.reductionFactor);
  matrix = vcat(matrixSectTime, matrixJoin) 
  dict   = merge(dictPhaseSectorPosition, dictJoin);
  return matrix, dict
end

function get_join_capacity_constraints(matrixSectTime::Array{Int,2}, 
                                       dictPhaseSectorPosition::Dict{String, Int},
                                       reductionFactor::Float64)
    sectLand = Set{String}();
    sectDep  = Set{String}();
    for k in keys(dictPhaseSectorPosition)
      if k[1] == 'l'
        push!(sectLand, k[6:end])
      elseif k[1] == 'd'
        push!(sectDep, k[5:end])
      end
    end
    sectJoin = intersect(sectLand, sectDep);
    position1 = size(matrixSectTime)[1];
    dictJoin = Dict("join/"*s => position1+idx for (idx, s) in enumerate(sectJoin));
    nCols = size(matrixSectTime)[2];
    nRows = length(sectJoin);
    matrixJoint = zeros(Int, nRows, nCols);
    for (idx, sector) in enumerate(sectJoin)
      posDep  = dictPhaseSectorPosition["dep/"*sector];
      posLand = dictPhaseSectorPosition["land/"*sector];
      valDep  = matrixSectTime[posDep,1];
      valLand = matrixSectTime[posLand,1];
      matrixJoint[idx,:] .= floor(Int, reductionFactor*(valDep+valLand));
    end
    return matrixJoint, dictJoin;
end

function assign_base_values!(matrixSectTime::Array{Int,2}, 
                             dictPhaseSectorPosition::Dict{String, Int}, 
                             ::Val{:default})
  nAir, nDep, nLand = compute_total_number_of_elements_in_each_category(dictPhaseSectorPosition);

  airValues = zeros(Int, nAir); landValues = zeros(Int, nLand); depValues = zeros(Int, nDep);
  compute_base_values!(matrixSectTime, dictPhaseSectorPosition, airValues, landValues, depValues);

  percentage = parameters.percentage;
  trimAir, trimLand, trimDep = compute_trimmed_mean!(airValues, landValues, depValues, percentage)
  
  assign_trimmed_means_as_minimum_values!(matrixSectTime, dictPhaseSectorPosition,
                                          trimAir, trimLand, trimDep);
end

function assign_trimmed_means_as_minimum_values!(matrixSectTime::Array{Int,2}, 
                                                 dictPhaseSectorPosition::Dict{String, Int}, 
                                                 trimAir::Int, 
                                                 trimLand::Int, 
                                                 trimDep::Int)
  for (k, i) in dictPhaseSectorPosition
    minimumValue = 0
    if k[1] == 'l'
      minimumValue = trimLand
    elseif k[1] == 'd'
      minimumValue = trimDep
    else
      minimumValue = trimAir
    end

    if matrixSectTime[i, 1] < minimumValue
      matrixSectTime[i,:] .= minimumValue;
    end
  end
end

function compute_trimmed_mean!(airValues::Array{Int,1}, 
                               landValues::Array{Int,1}, 
                               depValues::Array{Int,1},
                               percentage::Float64)
  sort!(airValues); sort!(landValues); sort!(depValues);
  nAir  = ceil(Int, length(airValues)*percentage/2);
  nDep  = ceil(Int, length(depValues)*percentage/2);
  nLand = ceil(Int, length(landValues)*percentage/2);

  trimAir  = ceil(Int, mean(airValues[nAir:(length(airValues)-nAir)]));
  trimDep  = ceil(Int, mean(depValues[nDep:(length(depValues)-nDep)]));
  trimLand = ceil(Int, mean(landValues[nLand:(length(landValues)-nLand)]));

  return trimAir, trimLand, trimDep;
end

function compute_base_values!(matrixSectTime::Array{Int,2}, 
                              dictPhaseSectorPosition::Dict{String, Int}, 
                              airValues::Array{Int,1}, 
                              landValues::Array{Int,1}, 
                              depValues::Array{Int,1})
  idxAir = idxLand = idxDep = 1;
  for (k, i) in dictPhaseSectorPosition
    maxVal = maximum(matrixSectTime[i,:]);
    matrixSectTime[i,:] .= maxVal;
    if k[1] == 'l'
      landValues[idxLand] = maxVal;
      idxLand += 1
    elseif k[1] == 'd'
      depValues[idxDep] = maxVal;
      idxDep += 1
    else
      airValues[idxAir] = maxVal;
      idxAir += 1
    end
  end
end

# The categories are: air, land, departure
function compute_total_number_of_elements_in_each_category(dictPhaseSectorPosition::Dict{String,Int})
  nAir = nDep = nLand = 0;
  for k in keys(dictPhaseSectorPosition)
    if k[1] == 'l'
      nLand += 1
    elseif k[1] == 'd'
      nDep += 1
    else
      nAir += 1
    end
  end
  return nAir, nDep, nLand
end

# A matrix where is row is a sector and each column a time period
function create_matrix_of_sector_usage(DF_3droutes::DataFrame)
  phaseSector = DF_3droutes[!, :phase] .* "/" .* DF_3droutes[!, :sector];
  sort!(phaseSector);
  unique!(phaseSector);
  dictPhaseSectorPosition = Dict(phaseSector[i] => i for i in 1:length(phaseSector));
  maxTime = maximum(DF_3droutes[!, :et]);

  matrixSectTime = zeros(Int, length(phaseSector), maxTime);
  for r in eachrow(DF_3droutes)
    idx = r[:phase] * "/" * r[:sector];
    row = dictPhaseSectorPosition[idx];
    times = r[:bt]:(r[:et] - 1);
    for t in times
      matrixSectTime[row, t] += 1;
    end
  end
  
  return matrixSectTime, dictPhaseSectorPosition;
end
