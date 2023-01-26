
# CREATES : dataframe called df_calls_orig which is a dataframe where ...
#           ROWS : represent call records
#           COLS : just the interesting variables

# -------------------------------------------------------------------------
# invalid records to remove

# 1. create a junk variable and set it to true for records to be junked
#    any records which are Initiated with a DISCONNECT status need to be junked
df_ctrs <- df_ctrs_orig %>% mutate(junk = case_when(InitiationMethod == 'DISCONNECT' ~ 1, T ~ 0))

# 2. some record sets have completely duplicated records - get rid of the whole recordset
df_junk <- df_ctrs %>% add_count(ctr_setid, InitiationTimestamp) %>% filter(n > 1) %>% select(ctr_setid)
df_ctrs <- df_ctrs %>% mutate(junk = case_when(ctr_setid %in% df_junk$ctr_setid ~ 2, T ~ junk))
rm(df_junk)

# 3. some subsequent records are transfers, but if the original record was updated last, junk the transfer
df_ctrs <- df_ctrs %>% 
  group_by(ctr_setid) %>% 
  arrange(ctr_setid, InitiationTimestamp) %>% 
  mutate(flag_final = LastUpdateTimestamp == max(LastUpdateTimestamp)) %>% 
  mutate(junk = case_when(junk == 0 & row_number() > 1 & flag_final == FALSE ~ 3, T ~ junk)) %>% 
  ungroup()

# these are the CTRs to junk
df_ctrs_junk <- df_ctrs %>% filter(junk > 0)

# These are good, although at this point we still have CTRs not calls.
# Junk some columns which aren't needed for analysis
# we need to know how many legs the call had and which of these each record represents

cols_junk <- c("junk", "AWSAccountId","AWSContactTraceRecordFormatVersion","AnsweringMachineDetectionStatus",
               "Agent","InstanceARN","MediaStreams","Recording","Recordings",
               "References", "VoiceIdResult", "Attributes.vmCalledService", "Attributes.vmReceiverEmail",
               "Attributes.vmSenderEmail", "Attributes.streamARN", "Agent.RoutingProfile.ARN", "Queue.ARN",
               "Recording.DeletionReason", "Recording.Location", "Recording.Status", "Recording.Type",
               "Attributes.Holidayname", "Attributes.publicHoliday", "Attributes.VMCustomerQueueflow",
               "Json Entry for Call","CustomerEndpoint.Type", "SystemEndpoint.Type", "Agent.ARN",
               "Agent.HierarchyGroups",
               "Agent.HierarchyGroups.Level1.ARN", 
               "Agent.HierarchyGroups.Level2.ARN", "Agent.HierarchyGroups.Level2",
               "Agent.HierarchyGroups.Level3.ARN", "Agent.HierarchyGroups.Level3",
               "Agent.HierarchyGroups.Level4.ARN", "Agent.HierarchyGroups.Level4",
               "Agent.HierarchyGroups.Level5.ARN", "Agent.HierarchyGroups.Level5", 
               "SystemEndpoint",
               "Attributes.ANI","Agent.StateTransitions",
               "Attributes.startFragmentNum","Attributes.endFragmentNum",
               "Attributes.FlowSelection",
               "Attributes.Language Check",
               "Attributes.VMCustomerQueueFlow",
               "Queue",
               "TransferredToEndpoint",
               "Campaign.CampaignId","TransferredToEndpoint.Type")

cols_order <- c("ctr_setid", "leg_count", "leg_id", 
                "flag_transfer","flag_final",
                "SystemEndpoint.Address", "Attributes.ServiceName", "InitiationMethod", "DisconnectReason",
                "InitialContactId", "ContactId", "PreviousContactId", "NextContactId", "Attributes.connectContactId",
                "InitiationTimestamp", "ConnectedToSystemTimestamp","ScheduledTimestamp", "TransferCompletedTimestamp", 
                "Queue.EnqueueTimestamp", "Queue.DequeueTimestamp", 
                "Agent.ConnectedToAgentTimestamp",  "Agent.AfterContactWorkStartTimestamp", 
                "DisconnectTimestamp", "LastUpdateTimestamp")

df_ctrs_good <- df_ctrs %>% 
  filter(junk == 0) %>% 
  select(-all_of(cols_junk)) %>% 
  group_by(ctr_setid) %>% 
  arrange(ctr_setid, InitiationTimestamp) %>% 
  mutate(leg_count = n(), .after = 2) %>% 
  mutate(leg_id = row_number(), .after = 3) %>% 
  mutate(flag_transfer = case_when(any(InitiationMethod == 'TRANSFER') ~ 1, 
                                   T ~ 0), .after = 4) %>% 
  mutate(flag_final = case_when(LastUpdateTimestamp == max(LastUpdateTimestamp) ~ 1,
                                T ~ 0)) %>% 
  select(all_of(cols_order), everything()) %>% 
  ungroup() 

# STILL TO DO : COLLAPSE MULTI-CTRS into a call record
df_calls_orig <- df_ctrs_good

# -------------------------------------------------------------------------
# check every multi-ctr scenario is accounted for

df_ctrs_good %>% 
  mutate(init_disc = paste(InitiationMethod, DisconnectReason)) %>% 
  xtab(init_disc, leg_id)

# -------------------------------------------------------------------------

# save to disk
WRITE_PARQUET <- TRUE
if (WRITE_PARQUET) {
  out.file <- here('data',glue('CALLS_{Sys.Date()}.parquet'))
  print(glue('Saving to {out.file}, {nrow(df_ctrs_good)} records in total'))
  write_parquet(df_calls_orig, out.file)
}
