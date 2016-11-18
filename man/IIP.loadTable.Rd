\name{IIP.loadTable}
\alias{IIP.loadTable}

\title{
Upload the file to IIP
}
\description{
upload the given file to a table in a workspace in IIP using IIP webservice
}
\usage{
IIP.loadTable(dataSource, workspaceName, columns, dataType, tableName, hdfsDelimiter, filepath, fileType,role)
}

\arguments{
  \item{dataSource}{
 Name of the IIP data source
}
  \item{workspaceName}{
 Name of the IIP workspace
}
  \item{columns}{
 Column names of the data frame to be uploaded
}
  \item{dataType}{
 Data types of all the coulmns of the data frame
}
  \item{tableName}{
 Name of the table that will appear in IIP
}
  \item{hdfsDelimiter}{
 The seperator to be used for storing table
}
  \item{filepath}{
 The exact location of file or the folder containing the files to be uploaded
}
  \item{fileType}{
 Type of table/file (default is csv)
}
\item{role}{
user role 
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
#IIP.loadTable(dataSource="DataSourceName", workspaceName="WorkspaceName", columns=c("Column1, Column2, Column3"), dataType=c("String, String, String"), tableName="TableName", hdfsDelimiter=",", fileType="csv", filepath = "ExactFileLocation", role = \"admin\")
}
