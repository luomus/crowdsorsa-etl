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

zip <- utils::unzip("data2023.zip", exdir = tmp)

data <- sf::st_read(tmp, quiet = TRUE)

response <- list()

for (i in seq_len(nrow(data))) {

  id <- data[[i, "id"]]

  municipality <- data[[i, "kuntakoodi"]]
  municipality <- switch(municipality, "864" = "20", municipality)
  municipality <- which(municipality_key_2023[["kunta"]] == municipality)
  municipality <- municipality_key_2023[[municipality, "municipality_name_fi"]]

  time <- strptime(data[[i, "havaittu"]], "%FT%T")

  date <- format(time, "%F")

  hour <- as.integer(format(time, "%H"))

  minute <- as.integer(format(time, "%M"))

  geo <- data[i, "geometry"]

  crs <- "WGS84"

  geo <- jsonlite::fromJSON(geojsonsf::sf_geojson(geo))

  geo[["type"]] <- jsonlite::unbox(geo[["type"]])

  taxon <- data[[i, "laji"]]

  taxon <- iconv(taxon, "latin1", "UTF-8")

  taxon <- gsub("Ã¶", "ö", taxon)

  taxon <- gsub("Ã¤", "ä", taxon)

  taxon_id <- switch(
    taxon,
    jättipalsami = "http://tun.fi/MX.39158",
    lupiini = "http://tun.fi/MX.38947",
    kurtturuusu = "http://tun.fi/MX.38815",
    jättiputki = "http://tun.fi/MX.41695",
    japanintatar = "http://tun.fi/MX.38240",
    karhunköynnös = "http://tun.fi/MX.5001859"
  )

  control_date <- data[[i, "torjunta"]]

  if (is.na(control_date)) {

    control <- list()

  } else {

    control <- list("INVASIVE_PARTIAL")

  }

  control_notes <- paste0("Controlled: ", control_date)

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

  if (!is.na(control_date)) {

    document[[c("publicDocument", "notes")]] <- jsonlite::unbox(control_notes)

  }

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
        reportedTaxonId = jsonlite::unbox(taxon_id),
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
  post <- httr2::req_body_json(post, document)

  message(sprintf("INFO [%s] Submitting document: %s", format(Sys.time()), id))

  response_i <- httr2::req_perform(post)

  if (response_i[["status_code"]] == 200L) {

    message(
      sprintf(
        "INFO [%s] Document %s submission successful", format(Sys.time()), id
      )
    )

  } else {

    message(
      sprintf(
        "ERROR [%s] Document %s submission unsuccessful (code: %s)",
        format(Sys.time()), id, response_i[["status_code"]]
      )
    )

  }

  response_i <- unclass(response_i)
  response_i[["cache"]] <- NULL
  response_i[["headers"]] <- unclass(response_i[["headers"]])
  response_i[["request"]] <- unclass(response_i[["request"]])

  response[[i]] <- response_i

  Sys.sleep(.1)

}

response <- jsonlite::toJSON(response, auto_unbox = TRUE, pretty = TRUE)

response <- gsub(Sys.getenv("FINBIF_ACCESS_TOKEN"), "", response)

cat(response, file = "logs.json")
