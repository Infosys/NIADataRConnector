
# (c) 2017 Infosys Limited. All rights reserved.
#  Except for any free or open source software  components embedded in this Infosys
#  Proprietary software program("Program"), this Program is  protected by copyright
#  laws, International treaties and other pending or existing intellectual property
#  rights in India,  the United  States  and other  countries.  Except as expressly
#  permitted, any unauthorized  reproduction  storage, photocopying,  recording  or
#  otherwise),  or any distribution  of  this  Program,  or any portion of  it, may
#  result  in  severe  civil  and criminal penalties, and will be prosecuted to the
#  maximum extent possible under the law.


# Authentication Type 
#                     - plain - password is stored in variable
#                     - encrpyt - Password is encrypted and is stored in file
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











