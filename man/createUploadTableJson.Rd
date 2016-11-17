\name{createUploadTableJson}
\alias{createUploadTableJson}

\title{
create the json object
}
\description{
create the json object using input parameters
}
\usage{
createUploadTableJson(dataSource, workspaceName, columns, dataType, hdfsDelimiter, filepath, tableName, fileType)
}
\arguments{
  \item{dataSource}{
  Name of the IIP data source

}
  \item{workspaceName}{
  Name of the IIP workspace

}
  \item{columns}{
  column names of the table to be uploaded
}
  \item{dataType}{
  data type of each column of the table to be uploaded
}
  \item{hdfsDelimiter}{
  The seperator to be used for storing table
}
  \item{filepath}{
hdfs location of the table
}
  \item{tableName}{
name of the table to be uploaded
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

"createUploadTableJson(dataSource=\"DataSourceName\", workspaceName=\"WorkspaceName\", columns=c(\"Column1,Column2,Column3\"), dataType=c(\"String,String,String\"), hdfsDelimiter=\",\", filepath=\"FilePath\", tableName=\"TableName\", fileType=\"csv\")"

}

