

# (c) 2015 Infosys Limited. All rights reserved.
#  Except for any free or open source software  components embedded in this Infosys
#  Proprietary software program("Program"), this Program is  protected by copyright
#  laws, International treaties and other pending or existing intellectual property
#  rights in India,  the United  States  and other  countries.  Except as expressly
#  permitted, any unauthorized  reproduction  storage, photocopying,  recording  or
#  otherwise),  or any distribution  of  this  Program,  or any portion of  it, may
#  result  in  severe  civil  and criminal penalties, and will be prosecuted to the
#  maximum extent possible under the law.


createUploadTableJson<-function(dataSource,workspaceName,columns,dataType,hdfsDelimiter,filepath,tableName, fileType, roleId)
{
  myObject<- list(saveInDataStore=dataSource,workspaceName=workspaceName,columns=columns,dataType=dataType,hdfsDelimiter=hdfsDelimiter,filepath=filepath,tableName=tableName,fileType=fileType, roleId = roleId)
  obj.json <- toJSON(myObject)
}

createGetFileLocationJson<-function(tableName,dataSource,workspaceName)
{
  myObject<-list(outputTableName =tableName,saveInDataStore=dataSource,workspaceName=workspaceName)
  obj.json <- toJSON(myObject)
  
}
authenticateWebServiceCall<-function(username,password)
{
  indexurl = Sys.getenv("IIP_INDEX_URL")
  loginurl = Sys.getenv("IIP_LOGIN_URL")
  pars=list( username=username, password=password)
  curl = getCurlHandle(verbose = TRUE)
  
  curlSetOpt( cookiejar="",followlocation = TRUE, curl=curl)
  getURL(indexurl, curl=curl)
  response=postForm(loginurl, .params = pars, curl=curl, style="post")
  return(curl)
}


IIP.getTableFilePath<-function(tableName,dataSource,workspaceName)
{
  
  if(Sys.getenv("SECURITY")=="TRUE")
  {
    ## true case - IIP.decrypt
    password<-IIP.decrypt(encryptedPwdFile = Sys.getenv("ENCRYPT_FILE"))
  }
  else
  {
    ## false case - fetch the password from Sys.getenv("PWD")
    
    password<-Sys.getenv("PWD")
    
  }
  
  credentials<-paste0(Sys.getenv("USERNAME"),":",password)
  
  file.obj<-createGetFileLocationJson(tableName,dataSource,workspaceName)
  curl=authenticateWebServiceCall(Sys.getenv("USERNAME"),password)
  
  
  options(RCurlOptions = list(cainfo = system.file("CurlSSL", "cacert.pem", package = "RCurl")))
  
  text = basicTextGatherer()
  header = basicHeaderGatherer()
  
  text$reset()
  
  if(Sys.getenv("IIP_VIEW_LOCATION_URL")==""){
    stop("Please ask the admin to set IIP_VIEW_LOCATION_URL")}
  
  curlPerform(url = Sys.getenv("IIP_VIEW_LOCATION_URL"),
              httpheader=c('Content-Type' = "application/json"),userpwd = credentials,
              curl=curl,
              useragent = "R",
              postfields=file.obj,
              writefunction = text$update,
              headerfunction = header$update,
              verbose = TRUE,
              ssl.verifypeer = FALSE
  )
  
  result = text$value()
  
  print(result)
  
  return(result)
}



copyToLocal<-function(tableName,dataSource,workspaceName)
{
  
  inputHdfsFile <- IIP.getTableFilePath(tableName, dataSource,workspaceName)
  localLocation <- createLocalDirectory(Sys.getenv("LOCAL_HOME"),paste("/input/",tableName,sep=""))
  system(paste("$HADOOP_CMD dfs -copyToLocal ", paste(inputHdfsFile,localLocation, sep = " "), sep = ""))
  print(paste0("File is created at ", localLocation))
  
  
  
}




IIP.JDBCConnection<-function (datasource="iip")
{
  library(RJDBC)
  .jinit()
  if (Sys.getenv("SECURITY") == "TRUE") {
    password <- IIP.decrypt(encryptedPwdFile = Sys.getenv("ENCRYPT_FILE"))
  }
  else {
    password <- Sys.getenv("PWD")
  }
  username <- Sys.getenv("USERNAME")
  options(java.parameters = "-Xmx8g")
  for (l in list.files(Sys.getenv("RJDBC_JARS"))) {
    .jaddClassPath(paste(Sys.getenv("RJDBC_JARS"), l, sep = ""))
  }
  if(datasource=="iip")
  {
    drv <- JDBC("org.apache.hive.jdbc.HiveDriver", Sys.getenv("HIVE_JDBC"))
    connection <- dbConnect(drv, Sys.getenv("JDBC_URL"), username,
                            password)
  } else if(datasource=="hive")
  {
    drv <- JDBC("org.apache.hive.jdbc.HiveDriver", Sys.getenv("HIVE_JDBC"))
    connection <- dbConnect(drv, Sys.getenv("HIVE_JDBC_URL"), username,
                            password)
  } else if(datasource=="mysql")
  {
    drv <- JDBC("com.mysql.jdbc.Driver",paste0(Sys.getenv("RJDBC_JARS"),"/",Sys.getenv("MYSQL_JAR")))
    connection <- dbConnect(drv, Sys.getenv("MYSQL_JDBC_URL"),Sys.getenv("MYSQL_USER"),Sys.getenv("MYSQL_PWD"))
  } else if(datasource=="postgres")
  {
    drv <- JDBC("org.postgresql.Driver",Sys.getenv("PSQL_JAR"))
    connection <- dbConnect(drv,Sys.getenv("PSQL_JDBC_URL"),Sys.getenv("PSQL_USER"),Sys.getenv("PSQL_PWD"))
  }
  connection
}



IIP.loadTable<-function(dataSource,workspaceName,columns,dataType,tableName,hdfsDelimiter,filepath,fileType, role)
{
  
  if(Sys.getenv("SECURITY")=="TRUE")
  {
    ## true case - IIP.decrypt
    password<-IIP.decrypt()
  }
  else
  {
    ## false case - fetch the password from Sys.getenv("PWD")
    
    password<-Sys.getenv("PWD")
    
  }
  
  
  
  credentials<-paste0(Sys.getenv("USERNAME"),":",password)
  
  curl=authenticateWebServiceCall(Sys.getenv("USERNAME"),password)
  
  userRole = getUserRole(Sys.getenv("USERNAME"),password,curl)
  
  id <- ""
  if(role %in% userRole$role)
  {
    id<-userRole$id[userRole$role==role]
  }
  
  
  if(id == ""){
    stop(paste0("The user - ",Sys.getenv("USERNAME"), "doesn't have", role, "privledges"))}
  
  options(RCurlOptions = list(cainfo = system.file("CurlSSL", "cacert.pem", package = "RCurl")))
  text = basicTextGatherer()
  header = basicHeaderGatherer()
  text$reset()
  # this line is required to access the table after registering, else the data path will come up with $USER_WORKSPACE/null as filepath will be #null
  #filepath<-ifelse(is.null(filepath),paste0(Sys.getenv("USER_WORKSPACE"),"/",fileName),filepath)
  
  
  json<-createUploadTableJson(dataSource,workspaceName,columns,dataType,hdfsDelimiter,filepath,tableName, fileType, id)
  
  
  if(Sys.getenv("IIP_VIEW_REGISTER_URL")==""){
    stop("Please ask the admin to set IIP_VIEW_REGISTER_URL")}
  
  curlPerform(url = Sys.getenv("IIP_VIEW_REGISTER_URL"),
              httpheader=c('Content-Type' = "application/json"),userpwd = credentials,
              curl=curl,
              useragent = "R",
              postfields=json,
              writefunction = text$update,
              headerfunction = header$update,
              verbose = TRUE,
              ssl.verifypeer = FALSE
  )
  result = text$value()
  print(result)
  ## Registration of Table
  schema<-paste0(unlist(strsplit(columns,",")),":",unlist(strsplit(dataType,",")),collapse=",")
  
  if(fileType== "parquet")
  {
    driverClassName<-"org.apache.spark.sql.parquet"
  }
  
  else
  {
    driverClassName<-"com.databricks.spark.csv"
  }
  # 
  
  
  
  query<-paste0('REGISTERJOB USING JSON { "name":','"',tableName,'"',
                ',"details": {
                "tables": [
{
                "schema":','"',schema,'"',",",
                '"delimiter":','"',hdfsDelimiter,'"',",\n",
                '"location": ','"',filepath,'"',",",
                '"fileType": ','"',fileType,'"',",",
                '"tableName": ','"',tableName,'"',",",
                '"driverClassName": ','"',driverClassName,'"
}
                ],
                "queries": [
{
                "query": "",
                "registerAsTable": "",
                "location": "",
                "materialized": false
}
                ]
}
}')

  
  #cred<-unlist(strsplit(credentials,":"))
  connection <- IIP.JDBCConnection()
  dbGetQuery(connection,query)
  query<-paste0("select count(*) from ",tableName)
  print(paste("Table",tableName," created with ",dbGetQuery(connection,query)," rows"))
  }


IIP.ParquettoTextTable<-function(tablename,location,query,sep="|",overwriteFlag=FALSE)
{
  if (hdfs.exists(location)==TRUE) {
    if(length(hdfs.ls(location)$file)>0 & overwriteFlag == FALSE)
    {
      stop("The specified location is not empty.")
    }
    else{
      connection<-IIP.JDBCConnection()
      myQuery<-paste0("CREATE TABLE ",tablename," ROW FORMAT DELIMITED FIELDS TERMINATED BY '",sep,"' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' STORED AS TEXTFILE LOCATION '",location,"' AS ",query)
      print(myQuery)
      dbGetQuery(connection,myQuery)
    }
  }
  else{
    connection<-IIP.JDBCConnection()
    myQuery<-paste0("CREATE TABLE ",tablename," ROW FORMAT DELIMITED FIELDS TERMINATED BY '",sep,"' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' STORED AS TEXTFILE LOCATION '",location,"' AS ",query)
    print(myQuery)
    dbGetQuery(connection,myQuery)
  }
}


IIP.uploadTable<-function(dataSource,workspaceName,hdfsDelimiter=",",dataFrame,tableName, fileType="csv", append = F, role)
{
  
  
  if (append == F)
  {
    #upload table
    
    uploadTableToIIP(dataSource,workspaceName,hdfsDelimiter,dataFrame,tableName, fileType, role)
    
  }
  
  else
  {
    
    #append table
    connection <- IIP.JDBCConnection()
    result<-tryCatch(dbGetQuery(connection,paste0("select count(*) from ",tableName)),
                     warning = function(w) {return("Warning message")},
                     error = function(e) {return("Table doesn't exists")})
    
    if(result == "Table doesn't exists")
    {
      #upload table
      
      print("table doesn't exists. uploading to IIP")
      uploadTableToIIP(dataSource,workspaceName,hdfsDelimiter,dataFrame,tableName, fileType, role)
    }
    
    else
    {
      
      tableDecription<-dbGetQuery(connection,paste0("Describe ",tableName)) 
      tableCol<-paste(tableDecription$col_name,collapse=",")
      
      columns<-paste((colnames(dataFrame)),collapse = ",")
      columns<-gsub("\\.","_",columns)
      factor<-sapply(dataFrame,is.factor)
      index<-as.numeric(which(factor==T))
      for(i in index)
      {    
        dataFrame[[i]] <- as.character(dataFrame[[i]])
      }
      dataType<-sapply(dataFrame,typeof)
      dataType<-gsub("character","string",gsub("logical","boolean",gsub("integer","int",dataType)))
      
      
      
      
      if(identical(tableCol,columns)== FALSE)
      {
        stop("The column names doesn't match with the table. Please make sure the data frame has same column names as the table")
      }
      
      dataType<- paste(dataType,collapse = ",")
      tableDataType<-paste(tableDecription$data_type,collapse=",")
      
      if(identical(tableDataType,dataType)== FALSE)
      {
        stop("The data type of column doesn't match with the table. Please make sure the data frame has same data type as the table")
      }
      
      curl=authenticateWebServiceCall(Sys.getenv("USERNAME"),password)
      userRole = getUserRole(Sys.getenv("USERNAME"),password,curl)
      if(role %in% userRole$role)
      {
        fileName=paste0("dataFrame.",fileType)
        tablePath=paste0(tableName,".",fileType)
        localFileLocation<-paste0(Sys.getenv("LOCAL_HOME"),"/",fileName)
        write.table(dataFrame,localFileLocation,row.names=F,col.names = F,sep =hdfsDelimiter)
        system (paste("$HADOOP_CMD fs -appendToFile",localFileLocation,paste0(Sys.getenv("USER_WORKSPACE"),"/",tablePath)))
        
        query<-paste0("select count(*) from ",tableName)
        print(paste("Table",tableName," appended and have a total of ",dbGetQuery(connection,query)," rows"))
        
      }
      
      else{
        stop(paste0("The user - ",Sys.getenv("USERNAME"), "doesn't have", role, "privledges")) 
        
      }
      
    }
    
  }
}


uploadTableToIIP<-function(dataSource,workspaceName,hdfsDelimiter=",",dataFrame,tableName, fileType="csv", role)
{
  
  
  if(Sys.getenv("SECURITY")=="TRUE")
  {
    ## true case - IIP.decrypt
    password<-IIP.decrypt(encryptedPwdFile = Sys.getenv("ENCRYPT_FILE"))
  }
  else
  {
    ## false case - fetch the password from Sys.getenv("PWD")
    
    password<-Sys.getenv("PWD")
    
  }
  
  credentials<-paste0(Sys.getenv("USERNAME"),":",password)
  
  fileName=paste0(tableName,".",fileType)
  localFileLocation<-paste0(Sys.getenv("LOCAL_HOME"),"/",fileName)
  
  if(length(colnames(dataFrame))==0){
    
    stop(paste0("Please provide the column names for the dataframe  ",deparse(substitute(dataFrame))))}
  
  
  #columns<-colnames(data)
  
  columns<-paste((colnames(dataFrame)),collapse = ",")
  columns<-gsub("\\.","_",columns)
  factor<-sapply(dataFrame,is.factor)
  index<-as.numeric(which(factor==T))
  for(i in index)
  {    
    dataFrame[[i]] <- as.character(dataFrame[[i]])
  }
  dataType<-sapply(dataFrame,typeof)
  dataType<-gsub("character","string",gsub("logical","boolean",gsub("integer","int",dataType)))
  
  
  dataType<- paste(dataType,collapse = ",")
  write.table(dataFrame,localFileLocation,row.names=F,col.names = F,sep =hdfsDelimiter)
  
  
  curl=authenticateWebServiceCall(Sys.getenv("USERNAME"),password)
  
  userRole = getUserRole(Sys.getenv("USERNAME"),password,curl)
  
  
  
  id <- ""
  if(role %in% userRole$role)
  {
    id<-userRole$id[userRole$role==role]
  }
  
  
  if(id == ""){
    stop(paste0("The user - ",Sys.getenv("USERNAME"), "doesn't have", role, "privledges"))}
  
  options(RCurlOptions = list(cainfo = system.file("CurlSSL", "cacert.pem", package = "RCurl")))
  text = basicTextGatherer()
  header = basicHeaderGatherer()
  text$reset()
  
  uploadToHDFS(localFileLocation,Sys.getenv("USER_WORKSPACE"))
  json<-createUploadTableJson(dataSource,workspaceName,columns,dataType,hdfsDelimiter,paste0(Sys.getenv("USER_WORKSPACE"),"/",fileName),tableName, fileType, id)
  
  
  
  if(Sys.getenv("IIP_VIEW_REGISTER_URL")==""){
    stop("Please ask the admin to set IIP_VIEW_REGISTER_URL")}
  
  curlPerform(url = Sys.getenv("IIP_VIEW_REGISTER_URL"),
              httpheader=c('Content-Type' = "application/json"),userpwd = credentials,
              curl=curl,
              useragent = "R",
              postfields=json,
              writefunction = text$update,
              headerfunction = header$update,
              verbose = TRUE,
              ssl.verifypeer = FALSE
  )
  result = text$value()
  print(result)
  
  
  ## Registration of Table
  
  schema<-paste0(unlist(strsplit(columns,",")),":",unlist(strsplit(dataType,",")),collapse=",")
  query<-paste0('REGISTERJOB USING JSON { "name":','"',tableName,'"',
                '"details": {
                "tables": [
{
                "schema":','"',schema,'"',",",
                '"delimiter":','"',hdfsDelimiter,'"',",\n",
                '"location": ','"',paste0(Sys.getenv("USER_WORKSPACE"),"/",fileName),'"',",",
                '"fileType": ','"',fileType,'"',",",
                '"tableName": ','"',tableName,'"',",",
                '"driverClassName": "com.databricks.spark.csv"
}
                ],
                "queries": [
{
                "query": "",
                "registerAsTable": "",
                "location": "",
                "materialized": false
}
                ]
}
}'
  )
  
  
  
  connection <- IIP.JDBCConnection()
  dbGetQuery(connection,query)
  
  res<-fromJSON(result)
  if(res=="SUCCESS"){
    query<-paste0("select count(*) from ",tableName)
    print(paste("Table",tableName," created with ",dbGetQuery(connection,query)," rows"))
  }
  else
  {
    print(" There was error in pushing the table to IIP")
  }
  
  }


getUserRole<-function(username,password, curl)
{
  
  
  
  
  options(RCurlOptions = list(cainfo = system.file("CurlSSL", "cacert.pem", package = "RCurl")))
  
  text = basicTextGatherer()
  header = basicHeaderGatherer()
  
  text$reset()
  
  if(Sys.getenv("IIP_RoleID_URL")==""){
    stop("Please ask the admin to set IIP_RoleID_URL")}
  
  
  credentials<-paste0(username,":",password)
  
  roleIdURL<- paste0(Sys.getenv("IIP_RoleID_URL"),"/",username,"/null")
  curlPerform(url = roleIdURL,
              httpheader=c('Content-Type' = "application/json"),userpwd = credentials,
              curl=curl,
              useragent = "R",
              writefunction = text$update,
              headerfunction = header$update,
              verbose = TRUE,
              ssl.verifypeer = FALSE
  )
  
  result = text$value()
  res<-fromJSON(result)
  
  
  
  roles <- read.table(text = "",col.names = c("id", "role"))
  
  for(i in 1:length(res$roles))
  { 
    temp<- c(res$roles[[i]]["roleId"],res$roles[[i]]["roleName"])
    roles[i,]<-temp
    
    
  }
  colnames(roles) = c("id", "role")
  
  return(roles)
}

