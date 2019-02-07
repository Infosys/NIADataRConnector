\name{IIP.getTableFilePath}
\alias{IIP.getTableFilePath}
\title{Get the HDFS location of a Nia Data table}
\description{
}
\usage{
IIP.getTableFilePath(tableName, dataSource, workspaceName)
}
\arguments{
  \item{tableName}{Name of the IIP table which contains the ingested data}
  \item{dataSource}{Name of the IIP data source}
  \item{workspaceName}{Name of the IIP workspace}
}
\details{}
\value{HDFS File path as a String}
\references{}
\author{
KiranKumar_Kaipa@infosys.com
Poorva_Agrawal@infosys.com
Raghavi_T01@infosys.com
}
\note{
}
\examples{
#IIP.getTableFilePath(tableName="TableName", dataSource="DataSourceName", workspaceName="WorkspaceName")
}
