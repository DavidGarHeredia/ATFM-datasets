
function create_rhs(DF_3droutes::DataFrame, parameters::Parameters)
  matrixSectTime, dictPhaseSectorPosition = create_base_scenario(DF_3droutes, parameters);
  dictSectorAirports, dictSectorSectors = get_relationships_for_penalizing(DF_3droutes)
  penlize_base_scenario!(matrixSectTime, dictPhaseSectorPosition, dictSectorAirports, parameters); 
  penalize_simulating_bad_weather!(matrixSectTime, dictSectorSectors, 
                        dictPhaseSectorPosition, dictSectorSectors, parameters); 
  set_min_capacity!(matrixSectTime);
  DF_rhs = transform_matrix_to_data_frame(matrixSectTime, dictPhaseSectorPosition);
  return DF_rhs;
end

function transform_matrix_to_data_frame(matrixSectTime::Array{Int,2},
                                  dictPhaseSectorPosition::Dict{String, Int})
    nRows, nCols = size(matrixSectTime)
    df = DataFrame(name = repeat([""], inner = nRows*nCols),
                   rhs  = zeros(Int, nRows*nCols))
    row = 1
    for (phaseSector, idx) in dictPhaseSectorPosition
      for t in 1:nCols
        df[row, :name] = phaseSector * "/" * string(t)
        df[row, :rhs] = matrixSectTime[idx, t]
        row += 1
      end
    end
    return df
end

function set_min_capacity!(matrixSectTime::Array{Int,2})
  nRows, nCols = size(matrixSectTime)
  for j in nCols, i in nRows
    matrixSectTime[i,j] = max(matrixSectTime[i,j], 1)
  end
end

function penalize_simulating_bad_weather!(matrixSectTime::Array{Int,2}, 
                                  dictSectorSectors::Dict{String, Set{String}},
                                  dictPhaseSectorPosition::Dict{String, Int},
                                  dictSectorAirports::Dict{String, Set{String}},
                                  parameters::Parameters)
  nCols = size(matrixSectTime)[2]
  duration = parameters.periodsOfWeatherPenalization
  sector = rand(keys(dictSectorSectors))
  reduction = parameters.badWeatherReduction
  for t0 in 1:duration:nCols
    for s in union([sector], dictSectorSectors[sector])
      phaseSector = "air/"*s
      pos = dictPhaseSectorPosition[phaseSector]
      tf = min(t0+duration, nCols)
      matrixSectTime[pos,t0:tf] .= floor.(Int, reduction * matrixSectTime[pos,t0:tf])
      airports = dictSectorAirports[s]
      for a in airports
        for operation in ["dep/", "land/", "join/"]
          key = operation*a
          if haskey(dictPhaseSectorPosition, key)
            pos = dictPhaseSectorPosition[key]
            matrixSectTime[pos,t0:tf] .= floor.(Int, reduction * matrixSectTime[pos,t0:tf])
          end
        end
      end
    end
    sector = rand(dictSectorSectors[sector])
  end
end

function get_sectors_and_indexes_base_penalization(dictPhaseSectorPosition::Dict{String, Int},
                                 percentageSectorsWithBasePenalization::Float64)
  rowIndexes = Array{Int,1}();
  sectors = Array{String,1}();
  for (phaseSector, position) in dictPhaseSectorPosition
    if phaseSector[1] == 'a' # air, i.e, not airport
      if rand() < percentageSectorsWithBasePenalization
        push!(rowIndexes, position)
        push!(sectors, phaseSector[5:end]) 
      end
    end
  end
  return sectors, rowIndexes;
end

function penalize_base_scenario!(matrixSectTime::Array{Int,2},
                                 dictPhaseSectorPosition::Dict{String,Int},
                                 dictSectorAirports::Dict{String, Set{String}}, 
                                 parameters::Parameters)
  sectorsToPenalize, rowIndexesToPenalize = 
            get_sectors_and_indexes_base_penalization(dictPhaseSectorPosition,
                                parameters.percentageSectorsWithBasePenalization)
  for (pos,idx) in enumerate(rowIndexesToPenalize)
    penalization = parameters.lbBasePenalization + rand()*(parameters.ubBasePenalization-parameters.lbBasePenalization)
    matrixSectTime[idx,:] .= floor.(Int, penalization*matrixSectTime[idx,:])
    sector = sectorsToPenalize[pos]
    if haskey(dictSectorAirports, sector)
      airportsInSector = dictSectorAirports[sector]
      for a in airportsInSector
        for operation in ["dep/", "land/", "join/"]
          key = operation*a
          if haskey(dictPhaseSectorPosition, key)
            row = dictPhaseSectorPosition[key]
            matrixSectTime[row,:] .= floor.(Int, penalization*matrixSectTime[row,:])
          end
        end
      end
    end
  end
end

is_an_airport(sector::String)= sector[1] == 'A';

function add_element_to_dict!(dict::Dict{String, Set{String}}, 
                              sector::String,
                              newSector::String)
  if !haskey(dict, sector)
    dict[sector] = Set{String}()
  end
  push!(dict[sector], newSector)
end

function get_relationships_for_penalizing(DF_3droutes::DataFrame)
  dictSectorAirports = Dict{String, Set{String}}();
  dictSectorSectors  = Dict{String, Set{String}}();
  N = nrow(DF_3droutes)-1
  for i in 2:N
    sector = DF_3droutes[i,:sector];
    if !is_an_airport(sector)
      for idx in [-1,1] # -1 prev sector, +1 next one
        newSector = DF_3droutes[i+idx,:sector];
        if is_an_airport(newSector)
          add_element_to_dict!(dictSectorAirports, sector, newSector);
        else
          add_element_to_dict!(dictSectorSectors, sector, newSector);
        end
      end
    end
  end
  return dictSectorAirports, dictSectorSectors
end

function create_base_scenario(DF_3droutes::DataFrame, parameters::Parameters)
  matrixSectTime, dictPhaseSectorPosition = create_matrix_of_sector_usage(DF_3droutes);
  assign_base_values!(matrixSectTime, dictPhaseSectorPosition);
  matrixJoin, dictJoin = get_join_capacity_constraints(matrixSectTime, 
                            dictPhaseSectorPosition, parameters.reductionFactorJoinConstraints);
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
                             dictPhaseSectorPosition::Dict{String, Int})
  nAir, nDep, nLand = compute_total_number_of_elements_in_each_category(dictPhaseSectorPosition);

  airValues = zeros(Int, nAir); landValues = zeros(Int, nLand); depValues = zeros(Int, nDep);
  compute_base_values!(matrixSectTime, dictPhaseSectorPosition, airValues, landValues, depValues);

  percentage = parameters.percentageForTrimmedMean;
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
