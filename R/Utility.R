# (c) 2019 Infosys Limited. All rights reserved.
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

authenticateWebServiceCall<-function(username, type = "POST")
{
  if (Sys.getenv("SECURITY") == "TRUE") {
    password <- IIP.decrypt(encryptedPwdFile = Sys.getenv("ENCRYPT_FILE"))
  }
  else {
    password <- Sys.getenv("PWD")
  }
  if(type == "POST"){curl.obj = curlpost}else{curl.obj = curlget}
  system(paste0("curl --noproxy '*' --insecure -o ./tgt.txt --data 'username=", username, "&password=",password , "' '", Sys.getenv("CAS_URL"), "'"))
  nodename = Sys.info()['nodename']
  system(paste0("echo TGT'grep -oEi 'action=\".*\"' tgt.txt | grep -oEi '\\-.*\\-", nodename, "'' > tgt.txt"))
  system(paste0("curl --noproxy '*' --insecure -o ./serviceTicket.txt  --data 'service=", Sys.getenv("IIP_CAS_SERVICE"), "' '", Sys.getenv("CAS_URL"), "'cat tgt.txt'"))
  #  system(paste0("curl --noproxy '*' -X HEAD -i --cookie-jar ./cookie.txt --data 'ticket=\'cat ./serviceTicket.txt\'' '", Sys.getenv("IIP_CAS_SERVICE"), "'"))
  curlSetOpt(cookiejar="./cookie.txt", useragent = "R", followlocation = TRUE, curl=curl.obj,verbose = TRUE,ssl.verifypeer = FALSE, httpheader=c('Content-Type' = "application/json"))
  curlPerform(url = Sys.getenv("IIP_CAS_SERVICE"),curl=curl.obj, readdata = 'ticket=\'cat ./serviceTicket.txt\'')
}

# Checks if the current session is active and returns it, otherwise creates a new session  
getActiveCURLHandle<- function(username, type = "POST", invalid_cert = F) {
  res = tryCatch(expr =  {if(invalid_cert ==T){stop("Invalid Certificate.")}
    if(type == "POST"){if((getCurlInfo(curlpost)[1]== "") || (!verifyCurlPost())){stop("Stale CURL Handle") }}else {if((getCurlInfo(curlget)[1]== "") || (!verifyCurlGet())){stop("Stale CURL Handle")}}},
    error=function(e) {
      #print(e)
      print("Creating a session...")
      curl.obj= getCurlHandle()
      curlSetOpt(cookiejar="./cookie.txt", followlocation = TRUE, curl=curl.obj, noproxy = "*")
      if(type == "POST"){
        assign("curlpost", curl.obj, envir = .GlobalEnv)
      }
      else {
        assign("curlget", curl.obj, envir = .GlobalEnv)
      }
      authenticateWebServiceCall(username = username, type)
    },
    warning=function(w) {
      #print(w)
      print("Creating a session...")
      curl.obj = getCurlHandle()
      curlSetOpt(cookiejar="./cookie.txt", followlocation = TRUE, curl=curl.obj, noproxy = "*")
      if(type == "POST"){
        assign("curlpost", curl.obj, envir = .GlobalEnv)
      }
      else {
        assign("curlget", curl.obj, envir = .GlobalEnv)
      }
      authenticateWebServiceCall(username = username, type)  },
    finally={
      if(type == "POST"){return(curlpost)}else{return(curlget)}
    }
  )    
}

#Function to check if current session is still valid
verifyCurlGet <- function() {
  url = paste0(Sys.getenv("IIP_RoleID_URL"), Sys.getenv("USERNAME"), "/null")
  tryCatch(expr = {result = getURL(url = url, curl=curlget, .opts = curlOptions(httpheader=c('Content-Type' = "application/json"), useragent = "R"));fromJSON(result);return(T)}, error = function(e){if(grepl("cannot open the connection", e, fixed = T) || grepl("Stale CURL handle", e, fixed = T)){return(F)}else{return(T)}})
}

verifyCurlPost <- function() {
  url = paste0(Sys.getenv("IIP_RoleID_URL"), Sys.getenv("USERNAME"), "/null")
  tryCatch(expr = {result = getURL(url = url, curl=curlpost, .opts = curlOptions(httpheader=c('Content-Type' = "application/json"), useragent = "R"));fromJSON(result);return(T)}, error = function(e){if(grepl("cannot open the connection", e, fixed = T) || grepl("Stale CURL handle", e, fixed = T)){return(F)}else{return(T)}})
}

IIP.getTableFilePath<-function(tableName,dataSource,workspaceName)
{
  file.obj<-createGetFileLocationJson(tableName,dataSource,workspaceName)  
  if(Sys.getenv("IIP_VIEW_LOCATION_URL")==""){
    stop("Please ask the admin to set IIP_VIEW_LOCATION_URL")}
  curl = tryCatch(expr = {getActiveCURLHandle(Sys.getenv("USERNAME"), type = "POST")}, error = function(e){getActiveCURLHandle(Sys.getenv("USERNAME"), type = "POST", invalid_cert = T)})
  text = basicTextGatherer()
  header = basicHeaderGatherer()
  text$reset()
  curlPerform(url = Sys.getenv("IIP_VIEW_LOCATION_URL"),
              httpheader=c('Content-Type' = "application/json"),
              curl=curl,
              useragent = "R",
              postfields=file.obj,
              writefunction = text$update,
              headerfunction = header$update,
              verbose = TRUE,
              ssl.verifypeer = FALSE
  )
  result = text$value()
  if(grepl(x = result, pattern = "IIP_1047", fixed = T))
    result = "FilePath Cannot Be Fetched"
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
    connection <- dbConnect(drv, Sys.getenv("HIVE_URL"), username,
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



IIP.loadTable<-function(dataSource,workspaceName,columns,dataType,tableName,hdfsDelimiter,filepath,fileType = "csv", role)
{
  userRole = tryCatch(expr = {getUserRole(Sys.getenv("USERNAME"))}, error = function(e){print("Encountered error while retrieving user Role");stop(e)})
  id <- ""
  if(role %in% userRole$role)
  {
    id<-userRole$id[userRole$role==role]
  }
  
  
  if(id == ""){
    stop(paste0("The user - ",Sys.getenv("USERNAME"), "doesn't have", role, "privledges"))}
  
  
  # this line is required to access the table after registering, else the data path will come up with $USER_WORKSPACE/null as filepath will be #null
  #filepath<-ifelse(is.null(filepath),paste0(Sys.getenv("USER_WORKSPACE"),"/",fileName),filepath)
 
  
  json<-createUploadTableJson(dataSource,workspaceName,columns,dataType,hdfsDelimiter,filepath,tableName, fileType, id)
  if(Sys.getenv("IIP_VIEW_REGISTER_URL")==""){
    stop("Please ask the admin to set IIP_VIEW_REGISTER_URL")}
  text = basicTextGatherer()
  header = basicHeaderGatherer()
  curl = tryCatch(expr = {getActiveCURLHandle(Sys.getenv("USERNAME"), type = "POST")}, error = function(e){print(e);getActiveCURLHandle(Sys.getenv("USERNAME"), type = "POST", invalid_cert = T)})
  text$reset()
  tryCatch(expr = {curlPerform(url = Sys.getenv("IIP_VIEW_REGISTER_URL"),
              httpheader=c("Content-Type" = "application/json"),
              curl=curl,
              useragent = "R",
              postfields= json,
              writefunction = text$update,
              headerfunction = header$update,
              verbose = TRUE,
              ssl.verifypeer = FALSE
  )}, warning = function(w){stop(w)}, error = function(e){stop(e)})
  result = text$value()
  res = ""
  tryCatch(expr = {res<-fromJSON(result)}, error = function(e){res<-result})
  if(res=="SUCCESS"){
    print(result)
  }
  query<-paste0("select count(*) from ",tableName)
  connection = IIP.JDBCConnection("hive")
  print(paste("Table",tableName," created with ",tryCatch(expr = {dbGetQuery(connection,query)}, warning = function(w){stop(w)}, error = function(e){stop(e)})," rows"))
  #return("Table",tableName," created with ",dbGetQuery(connection,query)," rows")
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
      dbSendUpdate(connection,myQuery)
    }
  }
  else{
    connection<-IIP.JDBCConnection()
    myQuery<-paste0("CREATE TABLE ",tablename," ROW FORMAT DELIMITED FIELDS TERMINATED BY '",sep,"' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' STORED AS TEXTFILE LOCATION '",location,"' AS ",query)
    print(myQuery)
    dbSendUpdate(connection,myQuery)
  }
}


IIP.uploadTable<-function(dataSource,workspaceName,hdfsDelimiter=",",dataFrame,tableName, fileType="csv", append = F, role)
{
  
  
  if (append == F)
  {
    #upload table
    uploadTableToIIP(dataSource,workspaceName,hdfsDelimiter,dataFrame,tableName, fileType, role,destination="hdfs")
    
  }
  
  else
  {
    
    #append table
    connection <- IIP.JDBCConnection()
    result<-tryCatch(expr = {dbGetQuery(connection,paste0("select count(*) from ",tableName))},
                     warning = function(w) {return("Warning message")},
                     error = function(e) {return("Table doesn't exists")})
    
    if(result == "Table doesn't exists")
    {
      #upload table
      
      print("table doesn't exists. uploading to IIP")
      uploadTableToIIP(dataSource,workspaceName,hdfsDelimiter,dataFrame,tableName, fileType, role,destination="hdfs")
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
      dataType<-gsub("character","string",gsub("logical","boolean",gsub("integer","int",dataType)))     
      
      
      
      
      if(identical(tableCol,columns)== FALSE){
        stop("The column names doesn't match with the table. Please make sure the data frame has same column names as the table")
      }
      
      dataType<- paste(dataType,collapse = ",")
      tableDataType<-paste(tableDecription$data_type,collapse=",")
      
      if(identical(tableDataType,dataType)== FALSE)
      {
        stop("The data type of column doesn't match with the table. Please make sure the data frame has same data type as the table")
      }
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
      userRole = tryCatch(expr = {getUserRole(Sys.getenv("USERNAME"))}, warning = function(w){print("Encountered Warning while retrieving user Role")}, error = function(e){print("Encountered error while retrieving user Role");stop(e)})
      if(role %in% userRole$role)
      {
        fileName=paste0("dataFrame.",fileType)
        tablePath=paste0("/",tableName,"/", tableName,".",fileType)
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


addRangerPolicy <- function(tableName, userName, roleName) {
  inputJson = paste0('{"policyName": "policy-', tableName,'","databases": "default","tables": "',tableName,'","columns": "*","udfs": "","description": "Hive Policy","repositoryName": "',Sys.getenv("RANGER_HIVE_REPOSITORY"),'","repositoryType": "',Sys.getenv("RANGER_HIVE_REPOSITORY_TYPE"),'","tableType": "Exclusion","columnType": "Inclusion","isEnabled": true,"isAuditEnabled": true,"permMapList": [{"groupList": ["',roleName,'"],"userList": ["',userName,'"],"permList": ["select","update"]}]}')
  curl.command = paste0('curl -iv -u ',Sys.getenv("RANGER_CREDENTIALS"),' -d \'',inputJson,'\' -H "Content-Type: application/json" ', Sys.getenv("RANGER_POLICY_URL"))
  system(curl.command, ignore.stdout = TRUE, ignore.stderr = TRUE)
}



IIP.uploadTableToHive<-function(dataSource,workspaceName,hdfsDelimiter=",",dataFrame,tableName, fileType, append = F, role)
{
  uploadTableToIIP(dataSource,workspaceName,hdfsDelimiter,dataFrame,tableName, fileType , role,destination="hive",append)
}

uploadTableToIIP<-function(dataSource,workspaceName,hdfsDelimiter=",",dataFrame,tableName, fileType="csv", role,destination,append=F)
{
  
  
  
  
  fileName=paste0(tableName,".",fileType)
  localFileLocation<-paste0(Sys.getenv("LOCAL_HOME"),"/",fileName)
  if(length(colnames(dataFrame))==0){
    stop(paste0("Please provide the column names for the dataframe  ",deparse(substitute(dataFrame))))}
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
  userRole = tryCatch(expr = {getUserRole(Sys.getenv("USERNAME"))}, warning = function(w){print("Encountered Warning while retrieving user Role")}, error = function(e){print("Encountered error while retrieving user Role");stop(e)})
  id <- ""
  if(role %in% userRole$role)
  {
    id<-userRole$id[userRole$role==role]
  }
  
  if(id == ""){
    stop(paste0("The user - ",Sys.getenv("USERNAME"), "doesn't have", role, "privledges"))}
  # Uploading to hive in default database.
  location<-paste0(Sys.getenv("USER_WORKSPACE"),"/",fileName)
  if(destination == "hive"){
    if(Sys.getenv("RANGER_POLICY_URL") != "" || !is.null(Sys.getenv("RANGER_POLICY_URL"))) {
      addRangerPolicy(tableName = tableName, userName = Sys.getenv("USERNAME"), roleName = role)
    }
    createHdfsDirectory(paste0(Sys.getenv("HADOOP_ADLS"),Sys.getenv("USER_WORKSPACE"),"/"),tableName)
    hdfsFileLocation<-paste0(Sys.getenv("HADOOP_ADLS"),Sys.getenv("USER_WORKSPACE"),"/",tableName,"/")
    #uploadToHDFS(localFileLocation,hdfsFileLocation)
    hdfs.put(localFileLocation,hdfsFileLocation)
    uploadToHive(hdfsFileLocation,tableName,columns,dataType,hdfsDelimiter,append)
    location<-paste0(Sys.getenv("HADOOP_ADLS"),Sys.getenv("HIVE_WAREHOUSE"),"/",tolower(tableName))
    fileType<-"hive"
    
    
    
    
    tableName<-paste0('default.',tableName)
    json<-createUploadTableJson(dataSource,workspaceName,columns,dataType,hdfsDelimiter, location,tableName, "hive", id)
  }
  else{
    hdfsFileLocation<-paste0(Sys.getenv("USER_WORKSPACE"),"/",tableName,"/")
    hdfs.mkdir(hdfsFileLocation)
    hdfs.put(localFileLocation,hdfsFileLocation)
    json<-createUploadTableJson(dataSource, workspaceName, columns, dataType, hdfsDelimiter, hdfsFileLocation, tableName, fileType, id)
  }
  if(Sys.getenv("IIP_VIEW_REGISTER_URL")==""){
    stop("Please ask the admin to set IIP_VIEW_REGISTER_URL")}
  text = basicTextGatherer()
  header = basicHeaderGatherer()
  curl = tryCatch(expr = {getActiveCURLHandle(Sys.getenv("USERNAME"), type = "POST")}, error = function(e){print(e);getActiveCURLHandle(Sys.getenv("USERNAME"), type = "POST", invalid_cert = T)})
  text$reset()
  curlPerform(url = Sys.getenv("IIP_VIEW_REGISTER_URL"),
              httpheader=c('Content-Type' = "application/json"),
              curl=curl,
              useragent = "R",
              postfields= json,
              writefunction = text$update,
              headerfunction = header$update,
              verbose = TRUE,
              ssl.verifypeer = FALSE
  )
  
  result = text$value()
  res = ""
  tryCatch(expr = {res<-fromJSON(result)}, error = function(e){res<-result})
  
  ##Fixing error log issue, commenting the FAILURE status
  if(res=="SUCCESS"){
    print(result)
  }
  query<-paste0("select count(*) from ",tableName)
  connection = IIP.JDBCConnection()
  print(paste("Table",tableName," created successfully with ",dbGetQuery(connection,query)," rows"))
}

getUserRole<-function(username)
{
  curl = tryCatch(expr = {getActiveCURLHandle(Sys.getenv("USERNAME"), type = "GET")}, error = function(e){print("Encountered authentication error");getActiveCURLHandle(Sys.getenv("USERNAME"), type = "GET", invalid_cert = T)})
  url = paste0(Sys.getenv("IIP_RoleID_URL"), username, "/null")
  tryCatch(expr = {result = getURL(url = url, curl=curl, .opts = curlOptions(httpheader=c('Content-Type' = "application/json"), useragent = "R"))}, error = function(e){print(e);print("Encountered error while retrieving userRole details");stop(e)})
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

## Upload table to Hive , without linking it to IIP
uploadToHive<-function(hdfsFileLocation,tableName,columns,dataType,hdfsDelimiter,append)
{
  
  connection <- IIP.JDBCConnection("hive")
  schema<-paste0(unlist(strsplit(columns,","))," ",unlist(strsplit(dataType,",")),collapse=",")
  createQuery<-paste0("CREATE TABLE IF NOT EXISTS ",tableName," (",schema,") ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                      WITH SERDEPROPERTIES (
                      'separatorChar' = '",hdfsDelimiter,"',
                      'quoteChar'     = '\"'
                      ) STORED AS TEXTFILE")
  print(createQuery)
  tryCatch(dbSendUpdate(connection,createQuery),
           warning = function(w) {print(w);print("Warning message")},
           error = function(e) {print(e);print("Table creation error")})
  if(append == F){
    insertQuery<-paste0("LOAD DATA INPATH '",hdfsFileLocation,"' OVERWRITE INTO TABLE ",tableName)
    print(insertQuery)
    tryCatch(dbSendUpdate(connection,insertQuery),
                 warning = function(w) {print(w);print("Warning message")},
                 error = function(e) { print(e);print("Table creation error")})
  }
  else{ 
    ## create temporary table to load incremental data into it
    dropTempTblQry<-paste0("DROP TABLE IF EXISTS ",tableName,"_temp")
    
    createTempTblQry<-paste0("CREATE TABLE IF NOT EXISTS ",tableName,"_temp(",schema,") ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                             WITH SERDEPROPERTIES (
                             'separatorChar' = '",hdfsDelimiter,"',
                             'quoteChar'     = '\"'
                             )  
                             STORED AS TEXTFILE ")
    loadtempTblQry<-paste0("LOAD DATA INPATH '",hdfsFileLocation,"' OVERWRITE INTO TABLE ",tableName,"_temp")
    insertQuery<-paste0("INSERT INTO TABLE ",tableName," SELECT * FROM ",tableName,"_temp")
    
    tryCatch(dbSendUpdate(connection,dropTempTblQry),
                 warning = function(w) {return("Warning message")},
                 error = function(e) { return("Temporary Table drop error")})
    
    tryCatch(dbSendUpdate(connection,createTempTblQry),
                 warning = function(w) {return("Warning message")},
                 error = function(e) { return("Temporary Table creation error")})           
    
    tryCatch(dbSendUpdate(connection,loadtempTblQry),
                 warning = function(w) {return("Warning message")},
                 error = function(e) { return("Temporary Table insertion error")})
    
    tryCatch(dbSendUpdate(connection,insertQuery),
                 warning = function(w) {return("Warning message")},
                 error = function(e) { return("Table insertion error")})

  }
  query<-paste0("select count(*) from ",tableName)
  print(paste("Table",tableName," has ",dbGetQuery(connection,query)," rows"))
}
