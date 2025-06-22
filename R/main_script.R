## Parte 1: GitHub ##

# install packages
#install.packages(c("data.table", "purrr", "furrr", "sf", "parallel", "arrow"), dependencies = TRUE)

# load packages
library(data.table)
library(purrr)
library(furrr)
library(sf)
library(parallel)
library(arrow)

## Parte 2: Paralelización ##

# 1. Descargar datos

# Instalar paquetes 
#install.packages(c("rvest", "httr"))

library(rvest)
library(httr)

# URL descarga
url <- "https://data-emf.creaf.cat/public/spatial_examples/exercise_4_2/"

# directorio destino
dest_dir <- "~/Documents/SACO/CienciaDatosGeograficos/Modulo4_AplicacionesYdesarrollosR/1_Entornos_desarrollo_colaborativo/big_data_training/raw_data"

# Leer HTML y extraer links y filtrar solo los gpkg
leer_url <- read_html(url)
links <- leer_url %>% html_nodes("a") %>% html_attr("href")
links <- links[grepl("\\.gpkg$", links)]

# bucle para construir URL completa y descargar
# for (f in links) {
#   file_url <- paste0(base_url, f)
#   dest <- file.path(dest_dir, f)
#   message("Descargando: ", f)
#   download.file(file_url, destfile = dest, mode = "wb")
# }

# provincia Zaragoza
# install.packages("mapSpain", dependencies = TRUE)
library(mapSpain)
zaragoza <- esp_get_prov_siane("Zaragoza")
zaragoza <- st_transform(zaragoza, crs = 25830)

# 2. Paralelilzacion

# install.packages(c("parallel","foreach","doParallel"),dependencies = TRUE)

library(tidyverse)
library(parallel)
library(foreach)
library(doParallel)

# extraccion de los valores deseados para 1 dia
file = "20250401.gpkg"
file_sin_ext <- sub("\\..*", "", file)
data <- sf::st_read(paste("raw_data/",file, sep = ""), 
                    query = paste('SELECT MeanTemperature, Precipitation, MeanRelativeHumidity, geom FROM "',file_sin_ext,'"', sep = ""),
                    wkt_filter = sf::st_as_text(st_geometry(zaragoza)),
                    quiet = TRUE)
#plot(data)

# lista de archivos a procesar
files <- list.files(path = "raw_data/", pattern = "\\.gpkg$")

# numero de cores
parallel::detectCores()

# determinar numero de cores
doParallel::registerDoParallel(cores = 6)

# leer datos en paralelo
system.time(
  {
data_union <- foreach::foreach(file = files) %dopar% {
  file_sin_ext <- sub("\\..*", "", file)
  data <- sf::st_read(paste("raw_data/",file, sep = ""), 
                      query = paste('SELECT MeanTemperature, Precipitation, MeanRelativeHumidity, geom FROM "',file_sin_ext,'"', sep = ""),
                      wkt_filter = sf::st_as_text(st_geometry(zaragoza)),
                      quiet = TRUE)
  data
}})
#data_union

# calcular valor medio
data_mean <- data_union %>%
  map(st_drop_geometry) %>% 
  reduce(`+`) / length(data_union)

# extraemos la gemoetria del primer elemento (ya que es igual para todos)
geom <- st_geometry(data_union[[1]])

# le damos de nuevo geometria a los valores medios
data_mean <- st_sf(data_mean, geometry = geom)

# 3. Guardar CSV

# convertimos de sf a wkt para tener las coordenadas de los puntos
data_mean_df <- data_mean %>%
  mutate(x = st_coordinates(.)[,1],
         y = st_coordinates(.)[,2]) %>%
  st_drop_geometry()

# guardar csv
write.csv(data_mean_df, "results/datos_medios_abr_25_zar.csv", row.names = TRUE)

