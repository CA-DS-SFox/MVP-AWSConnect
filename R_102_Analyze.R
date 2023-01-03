
library(tidyverse)
library(janitor)
library(here)
library(glue)

source('R_INCLUDE_Crosstab.R')        # code to do crosstabs and holecounts
source('R_INCLUDE_References.R')      # reference tables

# -------------------------------------------------------------------------
# read data

# get current totals file downloaded by script 101 from Jons data
local_dir <- here('data')

df_prod_downloads <- tibble(csvdata = list.files(local_dir, recursive = TRUE)) %>% 
  mutate(type = word(csvdata, 2, sep='\\.')) %>% 
  mutate(tag = word(csvdata, 1, sep='_')) %>% 
  mutate(date_from = word(csvdata, 2, sep='_')) %>% 
  mutate(date_to = word(csvdata, 3, sep='_')) %>% 
  mutate(date_to = word(date_to, 1, sep = '\\.')) %>% 
  filter(tag == 'CTR') %>% 
  # filter(date_from >= '2022-12-02') %>% 
  identity()

df_prod_downloads

# -------------------------------------------------------------------------
# make a combined dataset

for (i in seq_len(nrow(df_prod_downloads))) {
  
  in.file <- here('data',df_prod_downloads[i,c('csvdata')])

  if (i == 1) {
    df_ctrs_orig <- read_csv(in.file, col_types = cols(.default = 'c'))
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
  
# -------------------------------------------------------------------------
# rename to Max's variables names

# -------------------------------------------------------------------------
# source('R_INCLUDE_CallRecordDef.R')  # input CSVs of flattened JSON CTRs, output df_calls_orig which is a call based dataframe
# source('R_INCLUDE_CallTransforms.R)  # input df_calls_orig and output df_calls and df_calls_lastweek


