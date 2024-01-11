library(sf, warn.conflicts = FALSE, quietly = TRUE)

tmp <- tempfile()

zip <- unzip("esimerkkidata Nurmijärvi.zip", exdir = tmp)

data <- sf::st_read(tmp)
