
# dataset of all the timestamps in order that they happened
# and the duration in seconds between each timestamp

timestamps <- df_ctrs_clean %>% colnames() %>% str_subset(pattern = 'tm_')
durations <- df_ctrs_clean %>% colnames() %>% str_subset(pattern = 'dur_')

df_timestamps <- df_ctrs_clean %>% 
  select(ctr_setid, call_date, 
         leg_count, leg_id, leg_order, call, 
         AgentConnectionAttempts,
         transfer_count, transfer_actual, transfer_false,
         InitiationMethod, DisconnectReason,
         any_of(timestamps),
         any_of(durations)) %>% 
  identity()


# -------------------------------------------------------------------------
df_timestamps %>% 
  filter(InitiationMethod == 'DISCONNECT') %>% 
  count(tm_quenq)



# -------------------------------------------------------------------------

# no scheduled calls
df_timestamps %>% xtab(InitiationMethod, call)
df_timestamps %>% xtab(DisconnectReason, call)
df_timestamps %>% xtab(leg_count, call)

df_timestamps %>% 
  filter(leg_count > 1) %>% 
  View()

# -------------------------------------------------------------------------

notfirst <- df_timestamps %>% filter(leg_order == 1 & leg_id != 1)

df_timestamps %>% 
  filter(ctr_setid %in% notfirst$ctr_setid) %>% 
  View()
