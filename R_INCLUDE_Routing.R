
routing <- c('SystemEndpoint.Address',
             'TransferredToEndpoint.Address',
             'Attributes.FinalService',
             'Attributes.ServiceName','Attributes.AppName','Attributes.TransferName',
             'Queue.Name',
             'Agent.RoutingProfile.Name',
             'Agent.HierarchyGroups.Level1.GroupName',
             'Agent.HierarchyGroups.Level2.GroupName',
             'Agent.HierarchyGroups.Level3.GroupName',
             'Agent.HierarchyGroups.Level4.GroupName',
             'Agent.HierarchyGroups.Level5.GroupName')

df_routing <- df_ctrs_clean %>% 
  select(call_date, leg_id, InitiationMethod, tm_init, any_of(routing)) %>% 
  identity()

df_routing %>% count(leg_id, Attributes.ServiceName, Attributes.AppName) %>% print(n = 100)
