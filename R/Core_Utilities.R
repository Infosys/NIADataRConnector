
# Copyright 2016 Infosys Limited
# Licensed to the Software Freedom Conservancy (SFC) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The SFC licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.



getPasswordFromUser<-function()
{
  # get the password from user
  sysname<-Sys.info()[["sysname"]]
  ifRstudio<- Sys.getenv("RSTUDIO")
  if(ifRstudio == "1")
  {
    password<-.rs.askForPassword("")
    
    
  }
  
  else{
    if(sysname=="Linux")
    {
      password<-get_console_pwd()
    }
    else{
      password<-get_ui_pwd()
    }
  }
  
  return(password)
  
}

uploadToHDFS<-function(inputFilePath,hdfsFilePath)
{
  system (paste("$HADOOP_CMD fs -put -f",inputFilePath,hdfsFilePath))
}

deleteHdfsDirectory<-function(parentDir,dirName)
{
  path<-paste0(parentDir,dirName)
  system(paste0("$HADOOP_CMD dfs -rm -r ",path))
  path
}
deleteLocalDirectory<-function(dirName)
{
  dir<-paste(getwd(),dirName,sep="")
  #system(paste("$HADOOP_CMD dfs -rm -r ",dir,sep="")) ## delete the hdfs dir created from ngram code - by local path
  system(paste("rm -r ",dir,sep=""))
  dir
}
createHdfsDirectory<-function(parentDir,dirName)
{
  #TODO -error handling - if dir already exists then don't create one
  #TODO -error handling - if parent dir doesn't exists then create one /user/
  
  path<-paste0(parentDir,dirName)
  system(paste0("$HADOOP_CMD dfs -mkdir -p ",path))
  print(paste0("HDFS directory path is ",path))
  path
}
createLocalDirectory<-function(parentDir,dirName)
{
  #TODO -error handling - if dir already exists then don't create one
  #TODO -error handling - if dir doesn't exists then create one
  dir<-paste0(parentDir,dirName)
  system(paste0("mkdir -p ",dir))
  print(paste0("Local directory path is ",dir))
  dir
  
}

timeFormat<-function(timestamp, myPattern)
{
  timestamp<-sub(timestamp, pattern = " [[:alpha:]]*$", replacement = "")
  timestamp<-gsub(myPattern, "_", timestamp) 
  timestamp
}

readInputFile<-function(inputPath, readSep)
{
  data<-read.csv(pipe(paste("$HADOOP_CMD dfs -cat ",inputPath,sep="")),header = F,sep = readSep)
  data
  
}

getExactFilePath<-function(dirLocation)
{
  path<-hdfs.ls(dirLocation)
  if(length(path$file)==1)
  {
    return(path$file)
  }
  else
  {
    print(paste0("More than one file exists in the location - ",path$file,".  Please specify the exact file location and set it in inputHdfsFile paramater"))
  }
  
  
}


