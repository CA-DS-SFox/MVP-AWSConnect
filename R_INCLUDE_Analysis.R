library(tidyverse)
library(janitor)
library(arrow)
library(here)
library(glue)
library(googlesheets4)

TRANSFORMED.DATA <- TRUE
if (TRANSFORMED.DATA) {
  source('R_INCLUDE_Crosstab.R')        # code to do crosstabs and holecounts
  source('R_INCLUDE_References.R')      # reference tables
  df_calls <- read_parquet(here('data','TRANSFORMED.parquet'))
  
  print(max(df_calls$date_call))
}

# adhoc analysis of the data

# -------------------------------------------------------------------------
# daily CTRs by service

df_calls %>% 
  filter(when_date > '2023-01-01') %>% 
  count(when_date, service) %>% 
  rename(total_calls = n) %>% 
  ggplot(aes(when_date, total_calls, fill = service)) +
  theme(axis.text.x=element_text(angle=45,hjust=1)) +
  geom_col()

# -------------------------------------------------------------------------
# multiple calls

df_calls %>% 
  mutate(remote_number = CustomerEndpoint.Address) %>% 
  count(service, remote_number, sort = TRUE) %>% 
  slice(1:20) %>% 
  pivot_wider(names_from = service, values_from = n, values_fill = 0)

# -------------------------------------------------------------------------

tab_inbound <- df_calls_lastweek %>% 
  group_by(InitiationMethod, call_week, service) %>% 
  filter(flag_inbound == 1) %>% 
  summarise(in_vol = n(),
            in_abandoned = sum(flag_queued == 1 & flag_answer == 0),
            in_qabandon_time = mean(dur_enq_deq[flag_queued == 1 & flag_answer == 0], na.rm = TRUE),
            in_answered = sum(flag_answer == 1),
            in_qanswer_time = mean(dur_enq_deq[flag_queued == 1 & flag_answer == 1], na.rm = TRUE),
            in_call_time = mean(dur_call[flag_answer == 1]),
            in_aftercall_time = mean(dur_aft, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(across(where(is.numeric), ~coalesce(., 0)))
  
tab_outbound <- df_calls_lastweek %>% 
  group_by(InitiationMethod, call_week, service) %>% 
  filter(flag_outbound == 1) %>% 
  summarise(out_vol = n(),
            out_abandoned = sum(flag_answer == 0),
            out_abandon_time = mean(dur_total[flag_answer == 0], na.rm = TRUE),
            out_answered = sum(flag_answer == 1),
            out_qanswer_time = mean(dur_init_conn[flag_answer == 1], na.rm = TRUE),
            out_call_time = mean(dur_call[flag_answer == 1]),
            out_aftercall_time = mean(dur_aft, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(across(where(is.numeric), ~coalesce(., 0)))

# -------------------------------------------------------------------------

df_calltimes <- df_calls_lastweek %>% 
  count(InitiationMethod, call_week, call_day, call_hour) %>% 
  arrange(InitiationMethod, call_hour) %>% 
  pivot_wider(names_from = 'call_day', values_from = 'n', values_fill = 0) %>% 
  select("InitiationMethod","call_week","call_hour","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday",everything()) %>% 
  print(n = 100)

df_calls_lastweek %>% 
  count(okta.MBR, call_week, InitiationMethod) %>% 
  pivot_wider(names_from = 'InitiationMethod', values_from = 'n', values_fill = 0) %>% 
  print(n = 100)


# -------------------------------------------------------------------------

tab_answered <- df_calls %>% 
  group_by(call_week) %>% 
  summarise(date_from = min(call_date),
            date_to = max(call_date),
            total_calls = n(),
            answered_total = sum(flag_answer),
            answered_pcent = paste0(round(sum(flag_answer) / n() * 100, 1),'%'),
            total_inbound = sum(flag_inbound),
            answered_inbound_total = sum(flag_inbound == 1 & flag_answer == 1),
            answered_inbound_pcent = paste0(round(sum(flag_inbound == 1 & flag_answer == 1) / sum(flag_inbound) * 100, 1),'%'),
            total_outbound = sum(flag_outbound),
            answered_outbound_total = sum(flag_outbound == 1 & flag_answer == 1),
            answered_outbound_pcent = paste0(round(sum(flag_outbound == 1 & flag_answer == 1) / sum(flag_outbound) * 100, 1), '%'))
            

# -------------------------------------------------------------------------

df_calls %>% 
  filter(when_date > '2023-01-01') %>% 
  count(when_date) 


# df_calls %>% 
#   filter(when_date > '2023-01-01') %>% 
#   count(when_date, service) %>% 
#   rename(total_calls = n) %>% 
#   geom_bar(position = 'stack', stat='identity') +
#   geom_text(aes(label = total_Calls), vjust = -0.5, position = position_stack(vjust = .5)) +
#   scale_y_continuous(labels = label_number()) +
#   theme(axis.text.x=element_text(angle=45,hjust=1)) +
#   geom_col()

df_disk <- tribble(~when_date, ~disk_size,
                   "2023-01-01", 0.1,
                   "2023-01-02", 1.2,
                   "2023-01-03", 23.1,
                   "2023-01-04", 22.8,
                   "2023-01-05", 22.8,
                   "2023-01-06", 19.5,
                   "2023-01-07", .6,
                   "2023-01-08", .1,
                   "2023-01-09", 23.4,
                   "2023-01-10", 20.5,
                   "2023-01-11", 23.6,
                   "2023-01-12", 22.4,
                   "2023-01-13", 20.5,
                   "2023-01-14", .5,
                   "2023-01-15", .1,
                   "2023-01-16", 25,
                   "2023-01-17", 24.4,
                   "2023-01-18", 64.7)

df_disk %>% 
  ggplot(aes(when_date, disk_size)) +
  theme(axis.text.x=element_text(angle=45,hjust=1)) +
  geom_col()


# -------------------------------------------------------------------------

df_calls %>% 
  colnames()

# -------------------------------------------------------------------------

df_calls %>% 
  filter(flag_answer == 1) %>% 
  filter(service == 'Consumer') %>% 
  filter(!is.na(Attributes.MemberID)) %>% 
  filter(Attributes.MemberID != '#NA') %>% 
  left_join(df_mbr, by = c('Attributes.MemberID' = 'member_aws')) %>% 
  count(service, Attributes.MemberID, member_name, okta.MBR) %>% 
  add_count(member_name) %>% 
  filter(nn > 1) %>% 
  arrange(Attributes.MemberID) %>% 
  slice(1:20)

df_calls %>% 
  count(Attributes.ForMember)

df_calls %>% 
  mutate(Attributes.MemberID = na_if(Attributes.MemberID, "None")) %>% 
  mutate(Attributes.MemberID = na_if(Attributes.MemberID, "#NA")) %>% 
  mutate(dump = is.na(Attributes.MemberID) & is.na(Attributes.ForMember)) %>% 
  filter(!dump) %>% 
  count(Attributes.MemberID, Attributes.ForMember) 

df_calls %>% 
  filter(!is.na(oktaid)) %>% 
  slice(1:3) %>% 
  select(oktaid)

# -------------------------------------------------------------------------

hc <- c('Agent.HierarchyGroups.Level1.GroupName',
        'Agent.HierarchyGroups.Level2.GroupName',
        'Agent.HierarchyGroups.Level3.GroupName',
        'Agent.HierarchyGroups.Level4.GroupName',
        'Agent.HierarchyGroups.Level5.GroupName')

df_calls %>% hcount(cols_include = hc)

df_calls %>% 
  filter((!is.na(Agent.HierarchyGroups.Level4.GroupName) & (!is.na(okta_member_number)))) %>% 
  select(Agent.HierarchyGroups.Level4.GroupName, okta_member_number) %>% 
  count(Agent.HierarchyGroups.Level4.GroupName, okta_member_number) %>% 
  mutate(same = Agent.HierarchyGroups.Level4.GroupName == okta_member_number) %>% 
  arrange(same)
