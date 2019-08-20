\name{getActiveCURLHandle}
\alias{getActiveCURLHandle}
\title{
Creates an active GET/POST connection to connect with NiaData's APIs
}
\description{Creates an active GET/POST connection to connect with NiaData's APIs
}
\usage{
getActiveCURLHandle(username, type, invalid_cert)
}
\arguments{
  \item{username}{Nia authentication credential}
  \item{type}{Type of Request (POST by default)}
  \item{invalid_cert}{True, if the current curl handle is stale. False by default}
}
\value{Returns an Active CURL Handle}
\author{Raghavi_T01@infosys.com}
\examples{
#getActiveCURLHandle(username = "nia-user", type = "GET", invalid_cert = F)
}
