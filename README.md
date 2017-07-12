NIADataRConnector Package
========



## Introduction

The NIADataRConnector package has been built to allow data scientists and analysts access views created in IIP. 
It has many functions which enable various utilities like authentication of user, fetching of data & metadata from IIP and pushing data to HDFS and registering it is views/tables in IIP. 


 


## Various functions of the NIADataRConnector

 * ***Authenticate User*** - NIADataRConnector has a module that invokes the authentication service to authenticate the user to access data on IIPR. In build encryption is used to store passwords
 * ***Get Metadata*** - NIADataRConnector has a function that calls the web service to get metadata information about an IIP view/table. E.g. location of data on HDFS
 * ***Register view*** - Registers data on HDFS as a view/table in IIP by calling the view registration web service. 
 * ***Access the data*** – This allows the user to fetch the tables from IIP workspace. This can be done
    * By using standard Spark SQL queries leveraging standard JDBC connectors. 
    * By copying the data from HDFS location of the table to local machine
    * By reading the file directly from HDFS location of the table
 * ***Upload to HDFS***  - it allows the user to upload the R data as csv files to HDFS

## Installation

Get the latest stable development version from github:

```r
install_github("Infosys/NIADataRConnector")
```
    
## Example

```r
library(NIADataRConnector)
IIP.init("username")
IIP.uploadTable(dataSource = "testDataSource", workspaceName = "testWorkspace",hdfsDelimiter = ",", dataFrame= DataFrame object, tableName= "R_Table", fileType = "csv")
```



