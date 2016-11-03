\name{IIP.appendDfToTable}
\alias{IIP.appendDfToTable}

\title{
Append the data frame to an existing table in IIP
}
\description{
Append the given data frame to a table in a workspace in IIP using IIP webservice
}
\usage{
IIP.appendDfToTable(dataSource, workspaceName, hdfsDelimiter = ",", dataFrame, tableName, fileType = "csv")
}

\arguments{
  \item{dataSource}{
Name of the IIP data source
}
  \item{workspaceName}{
Name of the IIP workspace
}
  \item{hdfsDelimiter}{
The seperator to be used for storing table
}
  \item{dataFrame}{
Dataframe to be uploaded to IIP
}
  \item{tableName}{
name of the table in which data frame is  to be appended
}
  \item{fileType}{
 type of table/file (default is csv)
}
}
\details{

}
\value{

}
\references{

}
\author{
KiranKumar_Kaipa@infosys.com
Poorva_Agrawal@infosys.com
}
\note{

}



\seealso{

}
\examples{
#IIP.appendDfToTable(dataSource="DataSourceName", workspaceName="WorkspaceName", hdfsDelimiter = ",", dataFrame=DataFrameObject, tableName="TableName", fileType = "csv")

}