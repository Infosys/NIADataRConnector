\name{uploadToHive}
\alias{uploadToHive}
\title{uploadToHive}
\description{This program uses a file from the hdfs to create a Hive Table}
\usage{
uploadToHive(hdfsFileLocation, tableName, columns, dataType, hdfsDelimiter, append)
}
\arguments{
  \item{tableName}{Hive table name}
  \item{columns}{List of column names}
  \item{dataType}{List of data types}
  \item{hdfsDelimiter}{Text delimiter}
  \item{append}{True if we are uploading to an existing table}
  \item{hdfsFileLocation}{Text hdfs path to file}
}
\author{
Divya_VN@infosys.com
Raghavi_T01@infosys.com
}
\examples{
#uploadToHive(hdfsFileLocation = "hdfsFileLocation", tableName = "tableName", columns = c("column1", "column2"), dataType = "int, String", hdfsDelimiter = ",", append = T)
}
