\name{IIP.init}
\alias{IIP.init}
\title{
Initialize the IIP context
}
\description{
This function initialize the IIP context, import all important packages, create the scratch area for user and login with IIP. Once called, the function will prompt for password and then validate the user.
}
\usage{
IIP.init(username,password = NULL, encrypted = F)
}

\arguments{
  \item{username}{
Username of credential to connect to IIP instance. IIP login username.
}
\item{password}{
Password of credential to connect to IIP instance. IIP login password. leave blank if password is to be entered via pop-up.
}
\item{encrypted}{
whether the password provided is encrypted or not. If encrypted then set as TRUE otherwise FALSEo
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
#IIP.init("iipadmin")
}
