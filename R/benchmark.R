# librerias
library(tidyverse)
library(parallel)
library(foreach)
library(doParallel)

# lista de archivos a procesar
files <- list.files(path = "raw_data/", pattern = "\\.gpkg$")
files_3 <- files[1:3]

# benchmark
cores_benchmark <- bench::mark(
  core_2 = {
    doParallel::registerDoParallel(cores = 2)
    data_union <- foreach::foreach(file = files_3) %dopar% {
      file_sin_ext <- sub("\\..*", "", file)
      data <- sf::st_read(paste("raw_data/",file, sep = ""), 
                          query = paste('SELECT MeanTemperature, Precipitation, MeanRelativeHumidity, geom FROM "',file_sin_ext,'"', sep = ""),
                          wkt_filter = sf::st_as_text(st_geometry(zaragoza)),
                          quiet = TRUE)
      data}
  },
  core_4 = {
    doParallel::registerDoParallel(cores = 4)
    data_union <- foreach::foreach(file = files_3) %dopar% {
      file_sin_ext <- sub("\\..*", "", file)
      data <- sf::st_read(paste("raw_data/",file, sep = ""), 
                          query = paste('SELECT MeanTemperature, Precipitation, MeanRelativeHumidity, geom FROM "',file_sin_ext,'"', sep = ""),
                          wkt_filter = sf::st_as_text(st_geometry(zaragoza)),
                          quiet = TRUE)
      data}
  },
  core_6 = {
    doParallel::registerDoParallel(cores = 6)
    data_union <- foreach::foreach(file = files_3) %dopar% {
      file_sin_ext <- sub("\\..*", "", file)
      data <- sf::st_read(paste("raw_data/",file, sep = ""), 
                          query = paste('SELECT MeanTemperature, Precipitation, MeanRelativeHumidity, geom FROM "',file_sin_ext,'"', sep = ""),
                          wkt_filter = sf::st_as_text(st_geometry(zaragoza)),
                          quiet = TRUE)
      data}
  },
  iterations = 1, check = FALSE, memory = FALSE
)
cores_benchmark

# vemos que no hay mucha diferencia entre usar 4 o 6 cores, pero es más rapido que usar 2.