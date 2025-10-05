# #' @param indexes A character vector specifying which indexes to filter by. Options include "ASX", "B3", "SP500", and "Benchmark". If NULL, returns all symbols.

# # If indexes parameter is provided, filter the data accordingly
# valid_indexes <- c("ASX", "B3", "SP500", "Benchmark")
# if (!all(indexes %in% valid_indexes) | !is.null(indexes)) {
#   stop(paste(
#     "Invalid index provided. Valid options are:",
#     paste(valid_indexes, collapse = ", ")
#   ))
# }
