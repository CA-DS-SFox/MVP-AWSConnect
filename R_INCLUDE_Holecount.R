
cols_exclude <- 
  c(df_calls_lastweek %>% colnames()  %>% str_subset('tm_'), 
    df_calls_transform %>% colnames()  %>% str_subset('dur_'),
   "ctr_setid", "oktaid", "okta.name","call_minute", "call_second",
   "date_downloaded","date_call","phone.number")

x <- hcount(df_calls_lastweek, cols_exclude = cols_exclude)


df_calls_lastweek %>% colnames() %>% str_subset('Duration')

x <- df_calls_lastweek %>% 
  select("Agent.AfterContactWorkDuration",
         "dur_aft",
         "Agent.AgentInteractionDuration",
         "dur_call",
         "Agent.CustomerHoldDuration",
         "Agent.LongestHoldDuration") 
