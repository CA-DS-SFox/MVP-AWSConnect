

# adhoc analysis of the data

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
            
