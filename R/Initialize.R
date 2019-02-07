
# (c) 2019 Infosys Limited. All rights reserved.
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
IIP.init<-function(username,password = NULL)
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
    Sys.setenv(SECURITY=F)
   
    Sys.setenv("PWD" = password)
  }
  
  else
  {
    Sys.setenv(SECURITY=T)
    # encryption
    pwdFile<-paste0(Sys.getenv("USER_WORKSPACE"),"/.",username)
    Sys.setenv(ENCRYPT_FILE=pwdFile)
    if(hdfs.exists(pwdFile) & is.object(tryCatch(connect<-IIP.JDBCConnection(),error = function(err) return(length(err)))))
    {
      # directly fetch the file 
      Sys.setenv(ENCRYPT_FILE=pwdFile)
      
      
    }
    else
    {
      IIP.encrypt(pwd = getPasswordFromUser())
      #Sys.setenv(ENCRYPT_FILE=file)
    }
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
    if(!hdfs.exists(pwdFile)) {
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
  system(paste0("$HADOOP_CMD fs -put -f ~/.private.key ",Sys.getenv("USER_WORKSPACE")))
  keyPath<-paste0(Sys.getenv("USER_WORKSPACE"),"/.private.key")
  #create encryted password file
  if(!is.null(encryptedFilePath)) {
    #cat(encryptedPWD,file = encryptedFilePath, sep="\n")
    writeBin(encryptedPWD, encryptedFilePath)
    # write the file to hdfs
    system(paste("$HADOOP_CMD fs -put -f",encryptedFilePath,Sys.getenv("USER_WORKSPACE"), sep=" "))
    path<-encryptedFilePath
    #message(paste('Encrypted password file created at', encryptedFilePath, sep = " "))
  }
  else {
    #cat(encryptedPWD,file="encryptedPwd.txt",sep="\n")
    path<-paste0("~/.",Sys.getenv("USERNAME"))
    writeBin(encryptedPWD, path)
    system(paste("$HADOOP_CMD fs -put -f",path,Sys.getenv("USER_WORKSPACE"), sep=" "))
    #message(paste('Encrypted password file created at ',getwd(),"/encryptedPwd.txt",sep = ""))
  }
  Sys.setenv(ENCRYPT_FILE=paste0(Sys.getenv("USER_WORKSPACE"),"/.",Sys.getenv("USERNAME")))
  system("rm ~/.private.key")
  system(paste0("rm ~/.",Sys.getenv("USERNAME")))
  
  
  tryCatch(slaves<-readLines(paste0(Sys.getenv("SPARK_HOME"),"/conf/slaves")),
           warning = function(w) {return("Warning message")},
           error = function(e) {return("Unable to read slaves file")})
  tryCatch(lapply(slaves,function(x){system(paste0("scp ",path," ",Sys.getenv("USERNAME"),"@",x,":",path))}),
           warning = function(w) {return("Warning message")},
           error = function(e) {return("In case you are using IIPR as part of a R Map reduce job, please execute IIP.init(<username>) on each of the data nodes.")})
  tryCatch(lapply(slaves,function(x){system(paste0("scp ",keyPath," ",Sys.getenv("USERNAME"),"@",x,":",keyPath))}),
           warning = function(w) {return("Warning message")},
           error = function(e) {return("In case you are using IIPR as part of a R Map reduce job, please execute IIP.init(<username>) on each of the data nodes.")})
  
   
  #return(path)
}

#function to decrypt password files
IIP.decrypt <- function(encryptedPwdFile=NULL, privateKeyFile=NULL) {
  require(PKI)
  
  #check if arguments are null
  if(is.null(encryptedPwdFile)) {
    stop('encryptedPwdFile must be provided!')
  }
  if(!hdfs.exists(encryptedPwdFile)) {
    stop(paste(encryptedPwdFile,'file does not exists!',sep=" "))
  }
  if(is.null(privateKeyFile)) {
    keyPath<-paste0(Sys.getenv("USER_WORKSPACE"),"/.private.key")
    privateKeyFile <- pipe(paste0("$HADOOP_CMD dfs -cat ",keyPath))
    
  }
  
  #load private key
  private.key <- PKI.load.key(format = c("PEM","DER"),private = TRUE, what=privateKeyFile )
  
  #load encrypted pwd file
  system(paste0("$HADOOP_CMD dfs -copyToLocal ",encryptedPwdFile, " ~/.",Sys.getenv("USERNAME")))
  filePath<-paste0("~/.",Sys.getenv("USERNAME"))
  encryptedPwd <- readBin(filePath, "raw", file.info(filePath)$size * 1.1)
  system(paste0("rm ~/.",Sys.getenv("USERNAME")))
  
  #decrypt password
  decryptedPwd <- PKI.decrypt(encryptedPwd,private.key)
  
  return(rawToChar(decryptedPwd))
}

