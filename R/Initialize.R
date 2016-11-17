
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

IIP.init<-function(username)
{
  
  library(rhdfs)
  hdfs.init()
  library(RCurl)
  library(RJSONIO)
  library(RJDBC)
  Sys.setenv(SECURITY=T)
  sysname<-Sys.info()[["sysname"]]
  Sys.setenv(USERNAME=username)
   pwdFile<-paste0("~/.",username)
   Sys.setenv(ENCRYPT_FILE=pwdFile)
    if(file.exists(pwdFile) & is.object(tryCatch(connect<-IIP.JDBCConnection(),error = function(err) return(length(err)))))
    {
      # directly fetch the file 
      Sys.setenv(ENCRYPT_FILE=pwdFile)
      
    }
    else
    {
      # get the password from user
      ifRstudio<- Sys.getenv("RSTUDIO")
      if(ifRstudio == "1")
      {
        password<-.rs.askForPassword("")
        
        
      }
      
      else{
        if(sysname=="Linux")
        {
          password<-get_password()
        }
        else{
          password<- mask()
        }
      }
      
      IIP.encrypt(pwd = password)
      #Sys.setenv(ENCRYPT_FILE=file)
    }
   Sys.setenv(USERNAME=username)
  hdfsPath<-Sys.getenv("R_HDFS_HOME")
  if(hdfs.exists(hdfsPath))
  {
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
    stop(print(paste0("Please ask Hadoop admin to create the HDFS working directory"),hdfsPath))
  }
  localPath<-createLocalDirectory(paste0(getwd(),"/"),"data")
  
  Sys.setenv(LOCAL_HOME=localPath)
}

#for windows machines
mask <- function() {
  library(tcltk) 
  tt<-tktoplevel() 
  Password <- tclVar("") 
  entry.Password <-tkentry(tt,width="20",textvariable=Password,show="*") 
  tkgrid(tklabel(tt,text="Please enter your password.")) 
  tkgrid(entry.Password) 
  OnOK <- function() 
  { 
    tkdestroy(tt)  
    Password <<- tclvalue(Password) 
    cat("The password was ", Password, "\n") 
  } 
  OK.but <-tkbutton(tt,text="   OK   ",command=OnOK) 
  tkbind(entry.Password, "<Return>",OnOK) 
  tkgrid(OK.but) 
  tkfocus(tt) 
}

#for console
get_password <- function() {
  cat("Password: ")
  system("stty -echo")
  a <- readline()
  system("stty echo")
  cat("\n")
  return(a)
}


#function to encrypt passwords using key(RSA 2048bits) for encryption
IIP.encrypt <- function(pwd=NULL,pwdFile=NULL,encryptedFilePath=NULL) {
  require(PKI)
  
  #check if arguments are null
  if(any(is.null(pwd) & is.null(pwdFile))) {
    stop('pwd or pwdFile: any one argument is must!')
  }
  
  #check if pwd and pwdFile both are provided 
  if(any(!is.null(pwd) & !is.null(pwdFile))){
    stop('pwd & pwdFile: are mutually exclusive!')
  }
  
  if(!is.null(pwdFile)) {
    if(!file.exists(pwdFile)) {
      stop(paste(pwdFile,'file does not exists!',sep=" "))
    }
    else {
      pwd <- scan(file = pwdFile)
    }
  }
  
  #generating 2048-Bit RSA key
  key <- PKI.genRSAkey(2048)
  
  #Extracting private and public key
  private.key <- PKI.save.key(key)
  public.pem <- PKI.save.key(key, private = FALSE)
  public.key <- PKI.load.key(public.pem)
  
  #Encrypt using public key
  encryptedPWD <- PKI.encrypt(charToRaw(pwd),public.key)
  
  #internally store private key in a local file
  PKI.save.key(key, format = c("PEM","DER"), private = TRUE, paste0("~/.private.key"))
  keyPath<-paste0("~/.private.key")
  #create encryted password file
  if(!is.null(encryptedFilePath)) {
    #cat(encryptedPWD,file = encryptedFilePath, sep="\n")
    writeBin(encryptedPWD, encryptedFilePath)
    path<-encryptedFilePath
    #message(paste('Encrypted password file created at', encryptedFilePath, sep = " "))
  }
  else {
    #cat(encryptedPWD,file="encryptedPwd.txt",sep="\n")
    path<-paste0("~/.",Sys.getenv("USERNAME"))
    writeBin(encryptedPWD, path)
    
    #message(paste('Encrypted password file created at ',getwd(),"/encryptedPwd.txt",sep = ""))
  }
  Sys.setenv(ENCRYPT_FILE=path)
  slaves<-readLines(paste0(Sys.getenv("SPARK_HOME"),"/conf/slaves"))
  lapply(slaves,function(x){system(paste0("scp ",path," ",Sys.getenv("USERNAME"),"@",x,":",path))})
  lapply(slaves,function(x){system(paste0("scp ",keyPath," ",Sys.getenv("USERNAME"),"@",x,":",keyPath))})
  #return(path)
}

#function to decrypt password files
IIP.decrypt <- function(encryptedPwdFile=NULL, privateKeyFile=NULL) {
  require(PKI)
  
  #check if arguments are null
  if(is.null(encryptedPwdFile)) {
    stop('encryptedPwdFile must be provided!')
  }
  if(!file.exists(encryptedPwdFile)) {
    stop(paste(encryptedPwdFile,'file does not exists!',sep=" "))
  }
  if(is.null(privateKeyFile)) {
    privateKeyFile <- paste0("~/.private.key")
  }
  
  #load private key
  private.key <- PKI.load.key(format = c("PEM","DER"),private = TRUE, file=privateKeyFile)
  
  #load encrypted pwd file
  encryptedPwd <- readBin(encryptedPwdFile, "raw", file.info(encryptedPwdFile)$size * 1.1)
  
  #decrypt password
  decryptedPwd <- PKI.decrypt(encryptedPwd, private.key)
  
  return(rawToChar(decryptedPwd))
}









