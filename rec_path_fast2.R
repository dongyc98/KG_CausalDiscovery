library("SCCI")


dta <- read.csv(paste("/Users/dyc/Downloads/sample_data_collect_na.csv",sep=''), header=T)
# dta[is.na(dta)] <- 0
r <- 'rating'
x <- dta[[r]]
causes <- colnames(dta)
causes.tmp <- causes
for(cc in causes){
  if(grepl("wish", cc, fixed=TRUE)){
    causes.tmp <- causes.tmp[! causes.tmp %in% c(cc)]
  }
}
cause <- causes.tmp
n <- length(causes)
mins <- list()
for(r1 in causes){
  mins <- c(mins, list(r1 = 1e5))
}
names(mins) <- causes
for(cond in 0:n){
  causes <- causes.tmp
  
  if((cond + 2) > length(causes)){
    for(r1 in causes){
      print(paste(r,r1,-1,mins[[r1]],sep=' '))
    }
    break
  }
  
  for(r1 in causes){
    y <- dta[[r1]]
    if(r1==r){
      next
    }
    if(cond == 0){
      # independent test
      tmp.df <- data.frame(x,y)
      tmp.df <- tmp.df[complete.cases(tmp.df),]
      if(nrow(tmp.df) < 1000){
        print(paste("<1000",r,r1,sep=' '))
        scci <- 1e5
      }else{
        x0 <- tmp.df[['x']]
        y0 <- tmp.df[['y']]
        scci <- SCCI(x=x0, y=y0, Z=data.frame(rep(c(1),length(x0))), score="fNML", sym=FALSE)
      }
      if(scci == 0){
        print(paste(r,r1,0,sep=' '))
        causes.tmp <- causes.tmp[! causes.tmp %in% c(r1)]
        next
      }else{
        mins[[r1]] <- min(mins[[r1]], scci)
      }
    }else{
      # conditional independent test
      causes.new <- causes[! causes %in% c(r,r1)]
      for(com in 1:nrow(t(combn(causes.new,cond)))){
        tmp <- t(combn(causes.new,cond))[com,]
        z <- dta[tmp]
        # ...
        tmp.df <- data.frame(x,y,z)
        tmp.df <- tmp.df[complete.cases(tmp.df),]
        if(nrow(tmp.df) < 1000){
          print(paste("<1000",r,r1,paste(tmp,collapse=" "),cond,sep=' '))
          scci <- 1e5
        }else{
          x0 <- tmp.df[['x']]
          y0 <- tmp.df[['y']]
          z0 <- tmp.df[tmp]
          scci <- SCCI(x=x0, y=y0, Z=data.frame(z0), score="fNML", sym=FALSE)
        }
        if(scci==0){
          print(paste(r,r1,paste(tmp,collapse=" "),cond,sep=' '))
          causes.tmp <- causes.tmp[! causes.tmp %in% c(r1)]
          break
        }else{
          mins[[r1]] <- min(mins[[r1]], scci)
        }
      }
    }
  }
}



