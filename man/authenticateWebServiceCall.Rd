\name{authenticateWebServiceCall}
\alias{
authenticateWebServiceCall
}
\title{
Function to login to Nia Data
}
\description{CAS based Authentication
}
\usage{
authenticateWebServiceCall(username, type)
}
\arguments{
  \item{username}{
	Nia authentication credential
	}
  \item{type}{
	Type of Request (POST by default)
	}
}
\value{
No return value. Sets Cookie in the RCurl object.
}
\author{
Raghavi_T01@infosys.com
}
\examples{
#authenticateWebServiceCall(username = "nia-user", type = "POST")
}
