\name{uploadTableToIIP}
\alias{uploadTableToIIP}

\title{
Upload the data frame to IIP
}
\description{
upload the given data frame to a table in a workspace in IIP using IIP webservice
}
\usage{
uploadTableToIIP(dataSource, workspaceName, hdfsDelimiter = ",", dataFrame, tableName, fileType = "csv", role, destination, append)
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
name of the table to be uploaded
}
  \item{fileType}{
  type of table/file (default is csv)
}
 
 \item{role}{
user role
}
  \item{destination}{destination as hive or hdfs}
  \item{append}{boolean}
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
Raghavi_T01@infosys.com
}
\note{

}



\seealso{

}
\examples{
#uploadTableToIIP(dataSource="DataSourceName", workspaceName="WorkspaceName", hdfsDelimiter = ",", dataFrame=DataFrameObject, tableName="TableName", fileType = "csv", role ="admin")

}
