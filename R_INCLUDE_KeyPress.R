
keypress <- df_ctrs %>% colnames() %>% str_subset('Key')

df_keypress <- df_ctrs_clean %>% 
  #filter(dataset %in% c('HtC England','HtC Wales')) %>% 
  filter(str_detect(Attributes.FinalService,'Help to Claim' )) %>% 
  filter(call_date >= '2022-12-13') %>% 
  select(ctr_setid, call_date, dataset, leg_count, leg_id, keypress_var, keypress) %>% 
  identity()

df_keypress_tot <- df_keypress %>% 
  group_by(dataset, keypress_var, keypress) %>% 
  summarise(start = min(call_date),
            lastseen = max(call_date),
            total = n()) %>% 
  identity()

df_ctrs_clean %>% 
  filter(call_date >= '2022-12-13') %>% 
  #filter(Attributes.ServiceName == 'Help to Claim English') %>% 
  #filter(Attributes.KeyPress %in% c('- - #','- - X')) %>% 
  # count(Attributes.KeyPress) %>% 
  filter(InitiationMethod != 'DISCONNECT') %>% 
  count(dataset, InitiationMethod, Agent.RoutingProfile.Name, SystemEndpoint.Address, Queue.Name, Attributes.ServiceName, Attributes.FinalService) %>% 
  identity() %>% 
  print(n = 100)
