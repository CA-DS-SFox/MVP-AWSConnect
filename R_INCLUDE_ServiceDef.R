
# 2022-12-08 : STATUS : Needs input from Georgie

# This is where the definition of a per-service-dataset flag is to be created
# that will be how the data is split for analytical purposes

df_servicemap.1 <- df_calls %>% 
  filter(!is.na(Queue.Name)) %>% 
  group_by(Queue.Name) %>% 
  mutate(num_SystemEndPoint.Addresses = n_distinct(SystemEndpoint.Address)) %>% 
  ungroup() %>% 
  count(Queue.Name, num_SystemEndPoint.Addresses) %>% 
  #xtab(SystemEndpoint.Address, Queue.Name) %>% 
  #xtab(Queue.Name, num_SystemEndPoint.Addresses) %>% 
  rename(tot_QueueAddresses = n) %>% 
  print(n = 100)

df_servicemap.2 <- df_calls %>% 
  filter(!is.na(Queue.Name)) %>% 
  group_by(SystemEndpoint.Address) %>% 
  mutate(num_Queue.Name = n_distinct(Queue.Name)) %>% 
  ungroup() %>% 
  #xtab(SystemEndpoint.Address, Queue.Name) %>% 
  #xtab(SystemEndpoint.Address,num_Queue.Name) %>% 
  count(SystemEndpoint.Address, num_Queue.Name, Queue.Name) %>% 
  rename(tot_AddressQueues = n) %>% 
  arrange(SystemEndpoint.Address) %>% 
  print(n = 100)

df_servicemap.3 <- df_servicemap.2 %>% 
  left_join(df_servicemap.1, by='Queue.Name') %>% 
  select(SystemEndpoint.Address, Queue.Name, num_Queue.Name, num_SystemEndPoint.Addresses, tot_QueueAddresses, tot_AddressQueues) %>% 
  print(n = 100)

df_servicemap.4 <- df_calls %>% 
  xtab(SystemEndpoint.Address, Queue.Name) %>% 
  print(n = 100)


# -------------------------------------------------------------------------
# what defines answered ?

df_calls %>% 
  filter(!is.na(Queue.Name)) %>% 
  mutate(Agent.ConnectedToAgentTimestamp = as.POSIXct(Agent.ConnectedToAgentTimestamp)) %>% 
  mutate(Agent.AfterContactWorkStartTimestamp = as.POSIXct(Agent.AfterContactWorkStartTimestamp)) %>% 
  mutate(Agent.Duration = as.integer(Agent.AfterContactWorkStartTimestamp - Agent.ConnectedToAgentTimestamp)) %>% 
  select(Queue.Name, Queue.Duration, Agent.Username, Agent.Duration, Agent.ConnectedToAgentTimestamp, Agent.AfterContactWorkStartTimestamp, Agent.AfterContactWorkDuration)

