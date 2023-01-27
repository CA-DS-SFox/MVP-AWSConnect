library(tidyverse)
library(janitor)
library(arrow)
library(here)
library(glue)
library(googlesheets4)

source('R_INCLUDE_Crosstab.R')        # code to do crosstabs and holecounts
source('R_INCLUDE_References.R')      # reference tables
source('R_INCLUDE_Functions.R')        # code to do crosstabs and holecounts

# -------------------------------------------------------------------------

# read flattened CTRS
df_ctrs_all <- read_parquet(here('data','CTR_2023-01-23_2023-01-24.parquet'))
# df_ctrs_all <- read_parquet(here('data','TOTAL_2022-09-12_2023-01-25.parquet'))

# saved as ...
# df_calls <- read_parquet(here('data','CALLS_2023-01-23.parquet'))
# transform to calls
t_start <- Sys.time()
df_calls <- fn_CTR_to_CALL(df_ctrs_all)
t_end <- Sys.time()
t_length <- t_end - t_start
print(t_length)

# transform to analysis
t_start <- Sys.time()
df_analysis <- fn_CALL_to_ANALYSIS(df_calls)
t_end <- Sys.time()
t_length <- t_end - t_start
print(t_length)

