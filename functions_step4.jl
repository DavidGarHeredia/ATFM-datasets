
function create_rhs(DF_3droutes::DataFrame, parameters)
  matrixSectTime = create_base_scenario(DF_3droutes, parameters);
  # penlize_base_scenario!(matrixSectTime); # Habra parametros 3 base: none, medium, difficult
  # penalize_simulating_bad_weather!(matrixSectTime); # (blo) Habra parametros!!!
  # TODO: garantizar una cap min de 1
  # DF_rhs = transform_matrix_to_data_frame();
  return DF_rhs;
end

function create_base_scenario(DF_3droutes, parameters)
  matrixSectTime, dictPhaseSectorPosition = create_matrix_of_sector_usage(DF_3droutes);
  assign_base_values!(matrixSectTime, dictPhaseSectorPosition, Val(parameters.baseValuesRule));
  # add_join_capacity_constraints!(matrixSectTime, dictPhaseSectorPosition);
  # SOLO PARA CUANDO TENGO EL DEPARTURE Y ARRIVAL AIRPORT!!!
end

function assign_base_values!(matrixSectTime::Array{Int,2}, dictPhaseSectorPosition, ::Val{:default})
  nAir, nDep, nLand = compute_total_number_of_elements_in_each_category(dictPhaseSectorPosition);

  airValues = zeros(Int, nAir); landValues = zeros(Int, nLand); depValues = zeros(Int, nDep);
  compute_base_values!(matrixSectTime, dictPhaseSectorPosition, airValues, landValues, depValues);

  percentage = parameters.percentage;
  trimAir, trimLand, trimDep = compute_trimmed_mean!(airValues, landValues, depValues, percentage)
  
  assign_trimmed_means_as_minimum_values!(matrixSectTime, dictPhaseSectorPosition,
                                          trimAir, trimLand, trimDep);
end

function assign_trimmed_means_as_minimum_values!(matrixSectTime::Array{Int,2}, 
                                                 dictPhaseSectorPosition, 
                                                 trimAir, trimLand, trimDep)
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

function compute_trimmed_mean!(airValues, landValues, depValues, percentage)
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
                              dictPhaseSectorPosition, 
                              airValues, landValues, depValues)
  nrows, ncols = size(matrixSectTime);
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
function compute_total_number_of_elements_in_each_category(dictPhaseSectorPosition)
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
