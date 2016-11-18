library(IIPR)

####### There are 3 ways to initialize 

## 1
IIP.init("username","plain_text_password")
## 2
IIP.init("username")

## 3
IIP.init("username","encrypted_password", encrypted=T)

#### 

con<-IIP.JDBCConnection()
dbGetQuery(con, "show tables")
hive_con<-IIP.JDBCConnection("hive")
dbGetQuery(hive_con, "show tables")
sql_con<-IIP.JDBCConnection("mysql")
dbGetQuery(sql_con, "show tables")

IIP.getTableFilePath("tableName","ds-name","ws-name")

data = iris
IIP.uploadTable(dataSource = "ds-name",workspaceName = "ws-name",hdfsDelimiter=",",dataFrame=iris,tableName="iris", fileType="csv", append = F, role ="admin")
data = iris
IIP.uploadTable(dataSource = "ds-name",workspaceName = "ws-name",hdfsDelimiter=",",dataFrame=iris,tableName="iris", fileType="csv", append = TRUE, role ="admin")


