library(tidyverse)
library(arrow)
library(glue)
library(here)

WRITE_DATA = TRUE
WRITE_PARQUET = TRUE

# -------------------------------------------------------------------------
# get Jon's CSV exports 

source_dir <- 'G:/Shared drives/CA - Interim Connect Report Log Files & Guidance/Interim Reports/Contact Trace Records'

df_csvs <- tibble(csvdata = list.files(source_dir, recursive = TRUE)) %>% 
  mutate(type = word(csvdata, 2, sep='\\.')) %>% 
  mutate(day = word(csvdata, 3, sep='/')) %>% 
  mutate(day = substr(day,1,10)) %>% 
  filter(type == 'csv') %>% 
  identity()

print(glue('CSV from Jons process - {nrow(df_csvs)} in total from {min(df_csvs$day)} to {max(df_csvs$day)}'))

# -------------------------------------------------------------------------
# get current downloaded files and figure out which new ones to get
local_dir <- here('data')

df_downloads <- tibble(csvdata = list.files(local_dir, recursive = TRUE)) %>% 
  mutate(type = word(csvdata, 2, sep='\\.')) %>% 
  mutate(tag = word(csvdata, 1, sep='_')) %>% 
  mutate(date_from = word(csvdata, 2, sep='_')) %>% 
  mutate(date_to = word(csvdata, 3, sep='_')) %>% 
  mutate(date_to = word(date_to, 1, sep = '\\.')) %>% 
  filter(tag == 'CTR') %>% 
  identity()

print(df_downloads)
download_after <- max(df_downloads$date_to)

print(glue('Getting data after {download_after}'))

df_get <- df_csvs %>% filter(day > download_after)

print(glue('Getting CSV files - {nrow(df_get)} in total from {min(df_get$day)} to {max(df_get$day)}'))

# -------------------------------------------------------------------------
# get new data files

for (i in seq_len(nrow(df_get))) {
  
  thisfile <- paste0(source_dir,'/',df_get[i, c('csvdata')])
  thiscsv = read_csv(thisfile, col_types = cols(.default = 'c')) %>% mutate(date_download = Sys.time())
  
  if (i == 1) {
    totalcsv <- thiscsv
  } else {
    totalcsv <- bind_rows(totalcsv, thiscsv)
  }
  
  if (i %% 10 == 0) {
    print(paste0(i,' of ', nrow(df_get),' files processed ', nrow(totalcsv),' calls so far'))
  }
}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# remove first col which is an artefact from the python conversion export
outputcsv <- totalcsv %>% 
  select(-`...1`)

# get min and max days
df_dates <- df_get %>% 
  filter(day == max(day) | day == min(day)) %>% 
  select(day) %>% 
  mutate(type = case_when(day == min(day) ~ 'from',
                          T ~ 'to'))

date_from <- df_dates %>% filter(type == 'from') %>% pull(day)
date_to <- df_dates %>% filter(type == 'to') %>% pull(day)
if (length(date_to) == 0) date_to <- date_from

print(glue('Collected {nrow(df_get)} CSVs from {date_from} to {date_to}, {nrow(outputcsv)} records'))

# -------------------------------------------------------------------------
# save to file

if (WRITE_DATA) {
  # out.file <- here('data',glue('CTR_{date_from}_{date_to}.csv'))
  # print(glue('Saving to {out.file}'))
  # write.csv(outputcsv, out.file, row.names = FALSE)
  
  out.file <- here('data',glue('CTR_{date_from}_{date_to}.parquet'))
  print(glue('Saving to {out.file}'))
  write_parquet(outputcsv, out.file)
}

# -------------------------------------------------------------------------
# create total data in parquet format - need to make this more automated

if (WRITE_PARQUET) {
  local_dir <- here('data')
  
  df_parquets <- tibble(csvdata = list.files(local_dir, recursive = TRUE)) %>% 
    mutate(type = word(csvdata, 2, sep='\\.')) %>% 
    filter(type == 'parquet') %>% 
    mutate(tag = word(csvdata, 1, sep='_')) %>% 
    filter(tag == 'CTR') %>% 
    mutate(date_from = word(csvdata, 2, sep='_')) %>% 
    mutate(date_to = word(csvdata, 3, sep='_')) %>% 
    mutate(date_to = word(date_to, 1, sep = '\\.')) %>% 
    identity()
  
  date_from <- df_parquets %>% filter(date_from == min(date_from)) %>% pull(date_from)
  date_to <- df_parquets %>%  filter(date_to == max(date_to)) %>% pull(date_to)
  
  print(glue('Merging {nrow(df_parquets)} parquet files from {date_from} to {date_to}'))
  
  df_parquets
  
  # combined dataset
  for (i in seq_len(nrow(df_parquets))) {
    
    in.file <- here('data',df_parquets[i,c('csvdata')])
    
    if (i == 1) {
      df_ctrs_orig <- read_parquet(in.file) %>% mutate_all(as.character)
    } else {
      df_ctrs_orig <- dplyr::bind_rows(df_ctrs_orig, 
                                       df_ctrs_orig <- read_parquet(in.file) %>% mutate_all(as.character))
    }
    
    print(glue('File {i}, {in.file}, total rows {nrow(df_ctrs_orig)}, cols {ncol(df_ctrs_orig)}'))
  }
  
  # save to disk
  out.file <- here('data',glue('TOTAL_{date_from}_{date_to}.parquet'))
  print(glue('Saving to {out.file}, {nrow(df_ctrs_orig)} records in total'))
  write_parquet(df_ctrs_orig, out.file)
  
  # keep the latest TOTAL file only
  df_total_parquets <- tibble(totaldata = list.files(local_dir, recursive = TRUE)) %>% 
    mutate(type = word(totaldata, 2, sep='\\.')) %>% 
    filter(type == 'parquet') %>% 
    mutate(tag = word(totaldata, 1, sep='_')) %>% 
    filter(tag == 'TOTAL') %>% 
    mutate(date_from = word(totaldata, 2, sep='_')) %>% 
    mutate(date_to = word(totaldata, 3, sep='_')) %>% 
    mutate(date_to = word(date_to, 1, sep = '\\.')) %>% 
    arrange(desc(date_to)) %>% 
    filter(row_number() > 1) %>% 
    identity()
  
  df_total_parquets
  
  if (nrow(df_total_parquets) > 0) {
    for (del in df_total_parquets$totaldata) {
      del <- here('data', del)
      print(paste0(' .. Deleting', del))
      file.remove(del)
    }
  }
}
