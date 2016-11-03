\name{copyToLocal}
\alias{copyToLocal}

\title{
Copy the IIP table  to local
}
\description{
Given the table name, data source name and workspace name, this function fetch the hdfs location of the table and then copy it in the current working directory of user.
}
\usage{
copyToLocal(tableName, dataSource, workspaceName)
}
\arguments{
  \item{tableName}{
  Name of the IIP table which contains the ingested data 
}
  \item{dataSource}{
  Name of the IIP data source
}
  \item{workspaceName}{
  Name of the IIP workspace
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

#copyToLocal(tableName="TableName", dataSource="DataSourceName", workspaceName="WorkspaceName")

}

