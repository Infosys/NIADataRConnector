\name{IIP.uploadTableToHive}
\alias{IIP.uploadTableToHive}

\title{
Upload the data frame to IIP as a Hive Table
}
\description{
upload the given data frame to a table in a workspace in IIP as a Hive Table using IIP webservice
}
\usage{
IIP.uploadTableToHive(dataSource, workspaceName, hdfsDelimiter = ",", dataFrame, tableName, fileType, append = F, role)
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
 \item{append}{
  whether the table is to be appended or is to be created (default is csv)
}
 \item{role}{
user role
}
}
\details{
Created Managed Hive Tables
}
\value{

}
\references{

}
\author{
Divya_VN@infosys.com
Raghavi_T01@infosys.com
}
\note{

}



\seealso{

}
\examples{
#IIP.uploadTableToHive(dataSource="DataSourceName", workspaceName="WorkspaceName", hdfsDelimiter = ",", dataFrame=DataFrameObject, tableName="TableName", fileType = "csv", append = F, role ="admin")

}
