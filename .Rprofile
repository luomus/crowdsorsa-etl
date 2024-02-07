source("renv/activate.R")

if (dir.exists(".git")) {

  Sys.setenv(
    "BRANCH" = system("git symbolic-ref --short -q HEAD", intern = TRUE)
  )

}
