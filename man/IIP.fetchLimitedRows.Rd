\name{IIP.fetchLimitedRows}
\alias{IIP.fetchLimitedRows}

\title{
fetch a limited number of rows from datasource
}
\description{
Given the table name and data source name, this function fetches a limted number of rows from a hive table
}
\usage{
IIP.fetchLimitedRows(tableName, datasource = "iip")
}

\arguments{
  \item{tableName}{
Name of the table you want to fetch a sample from
}
  \item{datasource}{
Name of the datasource. hive/iip
}
}
\details{

}
\value{
}
\references{
}
\author{
kapil_kaushik@infosys.com
abhishek.paul@infosys.com
}
\note{
}

\seealso{
}
\examples{
#IIP.fetchLimitedRows(tableName, datasource = "iip") 
}