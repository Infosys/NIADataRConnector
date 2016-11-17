#Loading the IIPR package
library(IIPR)

#Initialise the IIPR instance using IIP.init function with username 
IIP.init("username")

#Method 1  -  Fetch IIP table from the Spark view/table using JDBC connection to thrift and then Query the Spark View in IIP
connection = IIP.JDBCConnection()
TestData<-dbGetQuery(connection,"select * from tablename")
dbDisconnect(connection)

#Method 2 -  Fetch the table through hdfs path
inputHdfsFilePath <-IIP.getTableFilePath(tableName = "tableName" ,dataSource = "dataSourceName",workspaceName = "workspaceName")
TableData<-readInputFile(inputPath = inputHdfsFilePath , readSep = "|" )

#Method 2 -   Copy the IIP table to local machine
copyToLocal(tableName = "tableName" ,dataSource = "dataSourceName",workspaceName = "workspaceName")

#Push the result back to IIP
IIP.uploadTable(dataSource = "testDataSource", workspaceName = "testWorkspace",hdfsDelimiter = ",", dataFrame= DataFrame object, tableName= "R_Table", fileType = "csv")
