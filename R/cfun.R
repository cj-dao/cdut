#' CSV Infix
#' 
#' Infix operator used for quickly defining csv dataframes and passing them
#' to a pipeline.
#'  
#' @param filepath Character type filepath without ".csv"
#' @param pipe Rest of %>% pipeline, enclosed in `()`
#' 
#' @details
#' Used when you want the object name to be the same as the csv name.
#' MUST be used with a pipeline. If you do not want to use a pipeline, use
#' the acsv function instead.
#' 
#' @examples
#' # Basic use:
#' "your_data" %csv% (
#'   select(...) %>%
#'   mutate(...) )
#' 
#' # The above is equivalent to:
#' your_data <- read.csv("your_data.csv") %>%
#'   select(...) %>%
#'   mutate(...)
#'   
#' @seealso \code{\link{acsv}}
#' 
#' @export
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

#' Assign CSV
#' 
#' Read a CSV and assign it to a variable of the same name. It also returns
#' the data frame, but isn't meant to be used in a pipeline.
#' 
#' @examples
#' # The following two lines of code are identical:
#' acsv("your_data")
#' your_data <- read.csv("your_data.csv")
#' 
#' @export
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

#' Trim
#' 
#' Removes columns and rows that are ALL blank or NA values. Returns a dataframe.
#' @export
trim <- function(data) {
  data <- data.frame(lapply(data, function(x) {
    x[x==""] <- NA
    return(x)
  }))
  return(data[rowSums(is.na(data))!=ncol(data),colSums(is.na(data))!=nrow(data)])
}

#' Collapse date and time
#' 
#' Combines date and time columns in character format
#' into a single, numerical column using the lubridate library.
#' 
#' @param dcol Date column (no quotes)
#' @param tcol Time column (no quotes)
#' @param format Optional. Call ?parse_date_time for more explanation.
#' @param rmdt If true, removes old date and time columns. Defaults to true.
#' 
#' @details 
#' \subsection{Basic use}{
#' This uses the parse_date_time function from the lubridate library to
#' better handle heterogeneous formats. It can handle multiple date formats,
#' even in the same data set. It returns a new data set with the specified
#' date and time columns merged and numericalized to the POISXct class.
#' }
#' \subsection{Formats}{
#' You can optionally specify formats using format="...". By default, this function
#' handles dmY, dmy, mdY, and mdy, prioritized in that order. It supports HMS time.
#' }
#' @export
collapse_dt <- function(data,dcol,tcol,format,rmdt) {
  dcol <- deparse(substitute(dcol))
  tcol <- deparse(substitute(tcol))
  if(missing(format)) {
    format <- c("dmY","dmy","mdY","mdy") %>%
      paste(c("HMS"))
  }
  if(missing(rmdt)) {
    rmdt <- TRUE
  }
  if(!(dcol %in% colnames(data) && tcol %in% colnames(data))) {
    stop("Make sure that the specified columns exist.")
  }
  # if(any(is.na(as.POSIXct(paste(data[,dcol],data[,tcol]),format=format)))) {
  #   warning("Some dates returned NA. Make sure that all the dates are in the same format, and that the format is correct. You can  manually set the format with 'format='.")
  # }
  dt_column <- data.frame(date_time = parse_date_time(x=paste(data[,dcol],data[,tcol]),orders=format))
  output <- cbind(dt_column,data)
  if(rmdt) {
    output <- select(output,-all_of(dcol),-all_of(tcol))
  }
  return (output)
}

#' Cut to times
#' 
#' Not recommended. Use \code{\link{merge_by_times}} instead.
#' 
#' @param data The main data set.
#' @param data_time_column The time column of main data set.
#' @param times The time column of the target data set
#' 
#' @details 
#' Use when you have two data sets with date and time columns,
#' and you wish to combine the data sets by matching each data point in the
#' main set to the closest time point in the target set. This function will
#' return a data set identical to the main set, but with the target time column
#' and with fewer data points.
#' 
#' @export
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

#' Merge by times
#' 
#' Matches one data set to another by closest time point.
#' 
#' @param data The main data set.
#' @param target_data The data set with times you want to match to.
#' @param tcol The date/time column of BOTH sets (name must be the same)
#' 
#' @details 
#' \subsection{Basic use}{
#' Use when you have two data sets with date and time columns,
#' and you wish to combine the data sets by matching each data point in the
#' main set to the closest time point in the target set. This function will
#' return the target data set with additional data columns from the main
#' data set.
#' }
#' 
#' \subsection{Notes:}{
#' Both data sets must have a date/time column with the same name, containing
#' dates in valid POSIXct format.
#' }
#' @export
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
