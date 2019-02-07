\name{IIP.ParquettoTextTable}
\alias{IIP.ParquettoTextTable}

\title{
Create a new table from the given query in the location provided
}
\description{
This function is to be used when temporary tables are to be created from existing Parquet files/ORC tables which cannot be pushed in R directly. The function will create a new plain text table which can then be pushed into IIP
}
\usage{
IIP.ParquettoTextTable(tablename, location, query, sep = "|", overwriteFlag = FALSE)
}

\arguments{
  \item{tablename}{
Name of the new table to be created
}
  \item{location}{
HDFS location where are the table/files will be created
}
  \item{query}{
Hive query to create the new table
}
  \item{sep}{
Separator to be used. (Default is pipe '|')
}
  \item{overwriteFlag}{
Whether or not to overwrite files in the specified location if any file already exists. Boolean. (Default is FALSE)
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
"IIP.ParquettoTextTable('ABCD', '/home/iipadmin/test/', 'select * from DEFG', sep = '|', overwriteFlag = TRUE)"
}