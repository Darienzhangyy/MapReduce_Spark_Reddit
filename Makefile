all: hw5.html

hw5.html: hw5.Rmd RankData/Top25.RData hourly.RData Feb_ThreeDays.RData
	Rscript -e "library(rmarkdown);render('hw5.Rmd')"

Rank_Data/Top25.RData: Reddit_Hadoop.R
	R --no-save < Reddit_Hadoop.R

hourly.RData: Task2_sparkR.R
	R --no-save < Task2_sparkR.R

Feb_ThreeDays.RData: Task3_spardoop.R
	R --no-save < Task3_spardoop.R

clean:
	rm -f hw5.html

.PHONY: all clean