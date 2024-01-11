suppressPackageStartupMessages({

  library(geofi, warn.conflicts = FALSE, quietly = TRUE)
  library(geojsonsf, warn.conflicts = FALSE, quietly = TRUE)
  library(httr2, warn.conflicts = FALSE, quietly = TRUE)
  library(jsonlite, warn.conflicts = FALSE, quietly = TRUE)
  library(sf, warn.conflicts = FALSE, quietly = TRUE)
  library(utils, warn.conflicts = FALSE, quietly = TRUE)

})

source_id <- "KE.1721"

collection_id <- "HR.5835"

url <- "https://api.laji.fi/"

if (Sys.getenv("BRANCH") != "main") {

  Sys.setenv(FINBIF_ACCESS_TOKEN = Sys.getenv("FINBIF_DEV_ACCESS_TOKEN"))

  url <- "https://apitest.laji.fi/"

  source_id <- "KE.1501"

}

tmp <- tempfile()

zip <- utils::unzip("data.zip", exdir = tmp)

data <- sf::st_read(tmp, quiet = TRUE)

response <- list()

for (i in nrow(data)) {

  id <- data[[i, "id"]]

  municipality <- data[[i, "kuntakoodi"]]
  municipality <- which(municipality_key_2023[["kunta"]] == municipality)
  municipality <- municipality_key_2023[[municipality, "municipality_name_fi"]]

  time <- strptime(data[[i, "havaittu"]], "%FT%T")

  date <- format(time, "%F")

  hour <- as.integer(format(time, "%H"))

  minute <- as.integer(format(time, "%M"))

  geo <- data[i, "geometry"]

  crs <- sf::st_crs(geo)[["input"]]

  # Add additional mappings if needed
  crs <- switch(crs, "WGS 84" = "WGS84", "WGS84")

  geo <- jsonlite::fromJSON(geojsonsf::sf_geojson(geo))

  geo[["type"]] <- jsonlite::unbox(geo[["type"]])

  taxon <- data[[i, "species"]]

  control <- data[[i, "torjunta"]]

  # Add additional mapping if needed
  control <- switch(control, list())

  document <- list(
    schema = jsonlite::unbox("laji-etl"),
    sourceId = jsonlite::unbox(sprintf("http://tun.fi/%s", source_id)),
    collectionId = jsonlite::unbox(sprintf("http://tun.fi/%s", collection_id)),
    documentId = jsonlite::unbox(
      sprintf("http://tun.fi/%s/%s", collection_id, id)
    )
  )

  document[["publicDocument"]] <- list(concealment = jsonlite::unbox("PUBLIC"))

  document[[c("publicDocument", "keywords")]] <- c(
    id, sprintf("crowdsorsa-%s", municipality)
  )

  document[[c("publicDocument", "gatherings")]] <- list()

  document[[c("publicDocument", "gatherings")]][[1L]] <- list(
    gatheringId = jsonlite::unbox(
      sprintf("http://tun.fi/%s/%s_G", collection_id, id)
    ),
    eventDate = list(
      begin = jsonlite::unbox(date), end = jsonlite::unbox(date)
    ),
    hourBegin = jsonlite::unbox(hour),
    hourEnd = jsonlite::unbox(hour),
    minuteBegin = jsonlite::unbox(minute),
    minuteEnd = jsonlite::unbox(minute),
    municipality = jsonlite::unbox(municipality),
    geo = list(
      type = jsonlite::unbox("FeatureCollection"),
      crs = jsonlite::unbox(crs),
      features = list(
        list(type = jsonlite::unbox("Feature"), geometry = I(geo))
      )
    ),
    units = list(
      list(
        unitId = jsonlite::unbox(
          sprintf("http://tun.fi/%s/%s_U", collection_id, id)
        ),
        taxonVerbatim = jsonlite::unbox(taxon),
        sourceTags = control
      )
    )
  )

  post <- httr2::request(url)
  post <- httr2::req_url_path_append(post, "v0")
  post <- httr2::req_url_path_append(post, "warehouse")
  post <- httr2::req_url_path_append(post, "push")
  post <- httr2::req_url_query(
    post, access_token = Sys.getenv("FINBIF_ACCESS_TOKEN")
  )
  post <- httr2::req_body_json(post, document, digits = 5L)

  response_i <- httr2::req_perform(post)

  response_i <- unclass(response_i)
  response_i[["cache"]] <- NULL
  response_i[["headers"]] <- unclass(response_i[["headers"]])
  response_i[["request"]] <- unclass(response_i[["request"]])

  response[[i]] <- response_i

}

response <- jsonlite::toJSON(response, auto_unbox = TRUE, pretty = TRUE)

response <- gsub(Sys.getenv("FINBIF_ACCESS_TOKEN"), "", response)

cat(response, "logs.json")
