

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


IIP.init<-function(username,password = NULL, encrypted = F)
{
  
  library(rhdfs)
  hdfs.init()
  library(RCurl)
  library(RJSONIO)
  library(RJDBC)
  
  
  Sys.setenv(USERNAME=username)
  hdfsPath<-Sys.getenv("R_HDFS_HOME")
  if(hdfs.exists(hdfsPath))
  {
    if(substr(hdfsPath,nchar(hdfsPath),nchar(hdfsPath))!="/")
    {
      hdfsPath<-paste0(hdfsPath,"/")
    }
    if(hdfs.exists(paste0(hdfsPath,username)))
    {
      print(paste0("Your HDFS working directory is  ",paste0(hdfsPath,username)))
    }
    else{
      hdfs.mkdir(path=paste0(hdfsPath,username))
      print(paste0("Your HDFS working directory is  ",paste0(hdfsPath,username)))
    }
    Sys.setenv("USER_WORKSPACE"=paste0(hdfsPath,username))  
  }
  else{
    stop(print(paste0("Please ask Hadoop admin to create the HDFS working directory",hdfsPath)))
  }
  localPath<-createLocalDirectory(paste0(getwd(),"/"),"data")
  
  Sys.setenv(LOCAL_HOME=localPath)
  
  # check for autheticatioon type
  
  if(!(is.null(password)))
  {
    if(encrypted == F)
     Sys.setenv("ENCRYPTED_PWD" = IIP.encrypt(password))
    else
      Sys.setenv("ENCRYPTED_PWD" = password)
  }
  
  else
  {
   Sys.setenv("ENCRYPTED_PWD"= IIP.encrypt(getPasswordFromUser()))
  }
 
}

#for windows machines
get_ui_pass<-function(){  
  library(tcltk);  
  window<-tktoplevel();
  password<-tclVar("");  
    
  tkgrid(tklabel(window,text="Enter password:"));  
   
  tkgrid(tkentry(window,textvariable=password,show="*")->passwordPopUp);  
   
  tkbind(passwordPopUp,"<Return>",function() tkdestroy(window));  
   
  tkgrid(tkbutton(window,text="OK",command=function() tkdestroy(window)));  
    
  tkwait.window(window);  
  pwd<-tclvalue(password);  
  return(pwd);  
}  


#for console
get_console_pwd <- function() {
  system("echo Enter Password: ")
  system("stty -echo")
  pwd <- readline()
  system("stty echo")
  #print("\n")
  return(pwd)
}



#function to encrypt passwords using key(RSA 2048bits) for encryption
IIP.encrypt <- function(password) {
  .jinit()
  .jaddClassPath(Sys.getenv("IIPR_CAS_JAR"))
  object<- .jnew("com.infosys.iipr.PasswordUtil")
  encryptedPWD <- .jcall(object, "S","encrypt",password)
  return(encryptedPWD)
}

#function to decrypt password files
IIP.decrypt <- function(encryptedPWD) {
  
  .jinit()
  .jaddClassPath(Sys.getenv("IIPR_CAS_JAR"))
  object<- .jnew("com.infosys.iipr.PasswordUtil")
  decryptedPwd <- .jcall(object, "S","decrypt",encryptedPWD)
  return(decryptedPwd)
}











