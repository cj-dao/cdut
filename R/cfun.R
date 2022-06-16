#' shorter infix version of acsv() for use in pipelines
#' use as follows:
#' "random_data" %csv% (
#'   function1() %>%
#'   function2() )
#' is equivalent to:
#' random_data <- read.csv("random_data.csv") %>%
#'   function1() %>%
#'   function2()
#' NOTE: pipeline MUST be in parentheses for %csv% infix to work!
`%csv%` <- function(filepath,pipe) {
  if(!exists("%>%")) {
    cat("Error in %csv%:\n  The pipeline infix operator '%>%' requires the magrittr library.\nImport magrittr library?\n(y/n)")
    response <- readline()
    if(response=="y") {
      library(magrittr)
      return (NULL)
    }
    stop("Pipeline infix operator not supported. Import dplyr or magrittr libraries.")
  }
  if(mode(filepath)!="character") {
    stop("Filepath must be a string.")
  }
  if(substr(filepath,nchar(filepath)-3,nchar(filepath))!=".csv") {
    filepath <- paste(filepath,".csv",sep="")
  }
  obj_name <- substr(filepath,1,nchar(filepath)-4)
  pipe_text <- deparse(substitute(pipe))
  if((substr(pipe_text,1,1)!="(" || substr(pipe_text,nchar(pipe_text),nchar(pipe_text))!=")")) {
    stop("Pipeline must be inside parenthesis!")
  }
  while (substr(pipe_text,1,1)=="(" && substr(pipe_text,nchar(pipe_text),nchar(pipe_text))==")") {
    pipe_text <- substr(pipe_text,2,nchar(pipe_text)-1)
  }
  exe_text <- paste("dataframe<-read.csv(filepath) %>%",pipe_text)
  eval(parse(text=exe_text))
  assign(obj_name,dataframe,1)
}

#' reads the csv at filepath (no ".csv" needed)
#' assigns dataframe to object with the same name as the csv then returns the object
#' e.g.: acsv("random_data") is equivalent to:
#' random_data <- read.csv("random_data.csv")
acsv <- function(filepath) {
  if(mode(filepath)!="character") {
    stop("Filepath must be a string.")
  }
  if(substr(filepath,nchar(filepath)-3,nchar(filepath))!=".csv") {
    filepath <- paste(filepath,".csv",sep="")
  }
  obj_name <- substr(filepath,1,nchar(filepath)-4)
  assign(obj_name,read.csv(filepath),1)
  return(get(obj_name))
}

#' removes columns and rows that are ALL blank or NA values
trim <- function(data) {
  data <- data.frame(lapply(data, function(x) {
    x[x==""] <- NA
    return(x)
  }))
  return(data[rowSums(is.na(data))!=ncol(data),colSums(is.na(data))!=nrow(data)])
}

#' combines date and time columns into a numerical column
#' dcol: date column; tcol: time column.
#' format: optional. Defaults to "%m/%d/%y %H:%M:%S"
#' rmdt: remove collapsed date and time columns? Defaults to true.
collapse_dt <- function(data,dcol,tcol,format,rmdt) {
  dcol <- deparse(substitute(dcol))
  tcol <- deparse(substitute(tcol))
  if(missing(format)) {
    format <- "%m/%d/%y %H:%M:%S"
  }
  if(missing(rmdt)) {
    rmdt <- TRUE
  }
  if(!(dcol %in% colnames(data) && tcol %in% colnames(data))) {
    stop("Make sure that the specified columns exist.")
  }
  if(any(is.na(as.POSIXct(paste(data[,dcol],data[,tcol]),format=format)))) {
    warning("Some dates returned NA. Make sure that all the dates are in the same format, and that the format is correct. You can  manually set the format with 'format='.")
  }
  dt_column <- data.frame(date_time = as.POSIXct(paste(data[,dcol],data[,tcol]),format=format))
  output <- cbind(dt_column,data)
  if(rmdt) {
    output <- select(output,-all_of(dcol),-all_of(tcol))
  }
  return (output)
}

#' selects timepoints from dataset that are closest in time to a shorter list
#' data: the dataset
#' data_time_column: the time column of the dataset to reference. No quotes needed.
#' times: list of times to cut down to
#' Note: "times" is a column, not the entire short dataset.
#' Note: the time column of the dataset will be replaced with the target times.
cut_to_times <- function(data,data_time_column,times) {
  
  # print("Working... Large datasets may take a while.")
  
  data_time_column <- deparse(substitute(data_time_column))
  lt <- data[,data_time_column]
  
  output <- filter(data,FALSE)
  
  for (i in 1:length(times)){
    
    time_diffs <- abs(difftime(times[i],lt,units="sec"))
    min_id <- which.min(time_diffs)
    new_row <- data[min_id,]
    # maxtd <- Inf
    # new_row <- data[1,]
    # for (j in 1:nrow(data)) {
    #   time_diff <- abs(difftime(times[i],lt[j],units="sec"))
    #   if(is.na(time_diff)) {
    #     stop("Time difference is NA. Note that times need to be in an acceptable POSIXct format.")
    #   }
    #   if(time_diff<maxtd) {
    #     maxtd <- time_diff
    #     new_row <- data[j,]
    #   }
    # }
    output <- rbind(output,new_row)
  }
  output <- mutate(output,cut_to_time = times) %>%
    select(-(all_of(data_time_column)))
  rownames(output) <- NULL
  return(output)
}

# data: larger dataset
# target_data: shorter dataset
# tcol: date_time column to reference (no quotes needed.)
# NOTE: the date_time column must have the same name for both datasets!
# returned dataset will have the dates from target_data and columns from both datasets.
merge_by_times <- function(data,target_data,tcol) {
  
  # print("Working... Large datasets may take a while.")
  
  tcol <- deparse(substitute(tcol))
  lt <- data[,tcol]
  data_timeless <- select(data,-all_of(tcol))

  output <- filter(data_timeless,FALSE)
  times <- target_data[,tcol]
  
  for (i in 1:length(times)){
    time_diffs <- abs(difftime(times[i],lt,units="sec"))
    min_id <- which.min(time_diffs)
    new_row <- data_timeless[min_id,,drop=FALSE]
    output <- rbind(output,new_row)
  }
  output <- cbind(target_data,output)
  return(output)
}
