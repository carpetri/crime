all:
	R -q -e 'library("rmarkdown"); setwd("$(CURDIR)"); render("crime.Rmd", output_format = "all");'
	# open -a 'google chrome' crime.html
	open -a 'Skim' crime.pdf