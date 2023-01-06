library(tidyverse)
library(arrow)
library(glue)
library(here)

WRITE.DATA = TRUE

# -------------------------------------------------------------------------
# get Jon's CSV exports 

source_dir <- 'G:/Shared drives/CA - Interim Connect Report Log Files & Guidance/Interim Reports/Contact Trace Records'

df_csvs <- tibble(csvdata = list.files(source_dir, recursive = TRUE)) %>% 
  mutate(type = word(csvdata, 2, sep='\\.')) %>% 
  mutate(day = word(csvdata, 3, sep='/')) %>% 
  mutate(day = substr(day,1,10)) %>% 
  filter(type == 'csv') %>% 
  identity()

print(glue('CSV files from Jons process - {nrow(df_csvs)} in total from {min(df_csvs$day)} to {max(df_csvs$day)}'))

# -------------------------------------------------------------------------
# get current total file
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

out.file <- here('data',glue('CTR_{date_from}_{date_to}.csv'))
print(glue('Saving to {out.file}'))
if (WRITE.DATA) write.csv(outputcsv, out.file, row.names = FALSE)


# -------------------------------------------------------------------------
# parquet format - need to make this more automated

if (WRITE_PARQUET) {
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
  
  # combined dataset
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
  
  outfile <- here('data','CTR_2022-09-12_2023-01-05.parquet')
  write_parquet(df_ctrs_orig, outfile)
}