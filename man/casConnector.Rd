\name{casConnector}
\alias{casConnector}

\title{
The R utility function which autherizes the web service calls with IIP CAS services
}
\description{
The function will authorize calls with IIP CAS services and get the user different responses
}
\usage{
casConnector(json, functionCall)
}

\arguments{
  \item{json}{
The json to be passed to the web service call
}
  \item{functionCall}{
the function call which is to be authorized -  
1. roleID - for fetching different roles of user
2. fileLocation - for fetching the HDFS location of a table in IIP
3. uploadTable - for uploading the table to IIP
}
}
\details{

}
\value{

}
\references{

}
\author{
poorva_agrawal@infosys.com, Prasanna_Teli@infosys.com
}
\note{

}



\seealso{

}
\examples{
#casConnector(json, "fileLocation")
}