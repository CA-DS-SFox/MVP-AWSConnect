library(tidyverse)
library(janitor)
library(arrow)
library(here)
library(glue)
library(googlesheets4)

# -------------------------------------------------------------------------
# get Duncan Ewart's current analysis

calls_in <- c("monthly",
              "week_num",
              "date",
              "total_inbound_calls",
              "offered_calls",
              "answered_calls",
              "percent_offered_calls_answered",
              "answered_within_20_seconds",
              "average_inbound_duration_mins",
              "queue_time_mins",
              "inbound_post_processing_time_mins",
              "calls_not_offered",
              "percent_calls_not_offered",
              "calls_offered_but_not_answered",
              "percent_calls_offered_but_not_answered",
              "percent_calls_answered")

calls_out <- c("monthly",
               "week_num",
               "date",
               "successful_outbound_calls",
               "average_outbound_duration_mins",
               "queue_time_mins",
               "outbound_post_processing_mins")

google_htc <- 'https://docs.google.com/spreadsheets/d/11tdsso1FGQn2qVnKCJA3wMeHZhvzeeRr6q7mwxlFYTA/edit#gid=0'

df_report_htc_orig <- read_sheet(google_htc)
df_report_htc <- as.data.frame(df_report_htc_orig)
df_report_htc <- df_report_htc %>% janitor::row_to_names(row_number = 1)
df_report_htc <- df_report_htc %>% janitor::clean_names()
x <- df_report_htc %>% colnames() %>% replace_na('source')
x <- replace(x, x == 'na', 'source')
df_report_htc <- df_report_htc %>% setNames(x)
df_report_htc %>% colnames()
df_report_htc <- df_report_htc %>% filter(is.na(source))

df_report_htc <- df_report_htc %>% 
  rowwise() %>% 
  unnest(cols = everything()) 

df_report_htc <- df_report_htc %>% mutate(date = as.Date(date))
df_report_htc <- df_report_htc %>% filter(date > Sys.Date() - 7)

df_report_htc_wide <- df_report_htc %>% 
  select(-source) %>% 
  t() %>% 
  as.data.frame() %>% 
  janitor::row_to_names(3, remove_rows_above = FALSE) %>% 
  tibble::rownames_to_column() %>% 
  mutate(source = 'interim', .before = 1) %>% 
  mutate(type = case_when(rowname %in% c('monthly', 'week_num') ~ 'info',
                          rowname %in% calls_in ~ 'inbound',
                          rowname %in% calls_out ~ 'outbound',
                          T ~ 'fix'), .before = 2)

df_report_htc_long <- df_report_htc_wide %>% 
  pivot_longer(-c('source', 'rowname', 'type'), names_to='date') %>% 
  mutate(value = round(as.numeric(value), 2)) %>% 
  identity()

# -------------------------------------------------------------------------
# get CTR data

# load CTRS and create analytical data using R_103_CreateAnalyticalData

df_htc <- df_analysis %>% 
  filter(dataset_htc == 1) %>% 
  mutate(across(starts_with('flag_'), as.integer)) 

# -------------------------------------------------------------------------
# inbound calls analysis

df_htc_in <- df_htc %>% 
  filter(flag_inbound == 1) %>% 
  filter(flag_weekday == 1) %>% 
  group_by(when_week, when_date) %>% 
  summarise(in_tot = n(),
            in_offered = sum(flag_queued),
            in_answered = sum(flag_answer),
            in_off_ans_pcent = round(sum(in_answered / in_offered), 2),
            in_ans_20 = sum(flag_answerin20), 
            in_calldur = round(mean(dur_call / 60, na.rm = TRUE)),
            in_queue = round(mean(dur_enq_deq / 60, na.rm = TRUE)),
            in_after = round(mean(dur_aft / 60, na.rm = TRUE)),
            in_nooffer = sum(flag_queuednot),
            in_nooff_pcent = round(sum(flag_queuednot / in_tot), 2),
            in_off_noans = sum(flag_queued == 1 & flag_answer == 0),
            in_off_noans_pcent = round(in_off_noans / in_tot, 2),
            in_ans_pcent = round(sum(in_answered / in_tot), 2)) %>% 
  ungroup() %>% 
  identity()

# outbound calls analysis
df_htc_out <- df_htc %>% 
  filter(flag_outbound == 1) %>% 
  filter(flag_weekday == 1) %>% 
  group_by(when_week, when_date) %>% 
  summarise(out_answered = sum(flag_answer),
            out_calldur = round(mean(dur_call / 60, na.rm = TRUE)),
            out_queue = round(mean(dur_enq_deq / 60, na.rm = TRUE)),
            out_after = round(mean(dur_aft / 60, na.rm = TRUE))) %>% 
  ungroup() %>% 
  identity()

df_htc_in
df_htc_out

# -------------------------------------------------------------------------
# create comparison

df_lookup <- tribble(~interim, ~pipeline,
                     "monthly","xx",
                     "week_num","yy",
                     "total_inbound_calls", "in_tot",
                     "offered_calls", "in_offered",
                     "answered_calls", "in_answered",
                     "percent_offered_calls_answered", "in_off_ans_pcent",
                     "answered_within_20_seconds", "in_ans_20",
                     "average_inbound_duration_mins", "in_calldur",
                     "queue_time_mins", "in_queue",
                     "inbound_post_processing_time_mins", "in_after",
                     "calls_not_offered", "in_nooffer",
                     "percent_calls_not_offered", "in_nooff_pcent",
                     "calls_offered_but_not_answered", "in_off_noans",
                     "percent_calls_offered_but_not_answered", "in_off_noans_pcent",
                     "percent_calls_answered", "in_ans_pcent",
                     
                     "successful_outbound_calls","out_answered",
                     "average_outbound_duration_mins","out_calldur",
                     "queue_time_mins","out_queue",
                     "outbound_post_processing_mins","out_after")

df_me_htc_long <- bind_rows(
  df_htc_in %>% 
    t() %>% 
    as.data.frame() %>% 
    janitor::row_to_names(2) %>% 
    tibble::rownames_to_column() %>% 
    mutate(source = 'pipeline', .before = 1) %>% 
    pivot_longer(-c('source', 'rowname'), names_to='date') %>% 
    mutate(value = round(as.numeric(value),2)) %>% 
    mutate(value = coalesce(value, 0)) %>% 
    left_join(df_lookup, by=c('rowname' = 'pipeline')) ,
  
  df_htc_out %>% 
    t() %>% 
    as.data.frame() %>% 
    janitor::row_to_names(2) %>% 
    tibble::rownames_to_column() %>% 
    mutate(source = 'pipeline', .before = 1) %>% 
    pivot_longer(-c('source', 'rowname'), names_to='date') %>% 
    mutate(value = round(as.numeric(value),2)) %>% 
    mutate(value = coalesce(value, 0)) %>% 
    left_join(df_lookup, by=c('rowname' = 'pipeline')) 
)

# comparison
rep_compare <- df_report_htc_long %>% 
  left_join(df_me_htc_long, by=c('rowname' = 'interim', 'date' = 'date')) %>% 
  filter(!is.na(source.y)) %>% 
  select(date, source.x, rowname, value.x, 
         source.y, rowname.y, value.y) %>% 
  mutate(diff = round((value.x - value.y) / value.x * 100, 1)) %>% 
  mutate(investigate = case_when(abs(diff) > 5 ~ 'Problem', T ~ '')) %>% 
  mutate(diff = paste0(diff, '%')) %>% 
  identity()

rep_compare %>% 
  filter(date == '2023-01-23')
