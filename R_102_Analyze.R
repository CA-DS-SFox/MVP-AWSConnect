library(tidyverse)
library(janitor)
library(arrow)
library(here)
library(glue)
library(googlesheets4)

source('R_INCLUDE_Crosstab.R')        # code to do crosstabs and holecounts
source('R_INCLUDE_References.R')      # reference tables

# -------------------------------------------------------------------------
# read data

# if we want to output CALLS_... from TOTAL_ ...
NEW.DATA <- TRUE

if (NEW.DATA) {
  
  local_dir <- here('data')
  
  df_prod_arrow <- tibble(datafiles = list.files(local_dir, recursive = TRUE)) %>% 
    mutate(type = word(datafiles, 2, sep='\\.')) %>%
    mutate(tag = word(datafiles, 1, sep='_')) %>%
    mutate(date_from = word(datafiles, 2, sep='_')) %>%
    mutate(date_to = word(datafiles, 3, sep='_')) %>%
    mutate(date_to = word(date_to, 1, sep = '\\.')) %>%
    filter(tag == 'TOTAL') %>%
    filter(type == 'parquet') %>% 
    filter(date_to == max(date_to)) %>%
    identity()
  
  df_prod_arrow
  
  for (i in seq_len(nrow(df_prod_arrow))) {
    
    in.file <- here('data', df_prod_arrow[i,c('datafiles')])
    
    if (i == 1) {
      df_ctrs_orig <- read_parquet(in.file, col_types = cols(.default = 'c'))
    } else {
      df_ctrs_orig <- dplyr::bind_rows(df_ctrs_orig,
                                       read_csv(in.file, col_types = cols(.default = 'c')))
    }
    
    print(glue('File {i}, {in.file}, total rows {nrow(df_ctrs_orig)}, cols {ncol(df_ctrs_orig)}'))
  }
  
  # we need a contactid for the set of records that happened
  df_ctrs_orig <- df_ctrs_orig %>% 
    mutate(ctr_setid = case_when(is.na(InitialContactId) ~ ContactId, T ~ InitialContactId), .before = 1) %>% 
    mutate(junk = 0, .before = 2)

  # now run this to create df_ctrs_good and save to parquet
  # source('R_INCLUDE_CallRecordDef.R')  # input CSVs of flattened JSON CTRs, output df_calls_orig which is a call based dataframe
  
} else {
  
  # bypass R_INCLUDE_CallRecordDef.R and pick up data from here
  in.file <- here('data', 'CALLS_2023-01-20.parquet')
  df_calls_orig <- read_parquet(in.file, col_types = cols(.default = 'c'))
}  

# -------------------------------------------------------------------------
# now run
# source('R_INCLUDE_CallTransforms.R)  # input df_calls_orig and output df_calls and df_calls_lastweek


