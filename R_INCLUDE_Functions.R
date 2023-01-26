
# transform a dataframe of ctrs to a dataframe of calls
# CREATES : dataframe called df_calls_orig which is a dataframe where ...
#           ROWS : represent call records
#           COLS : just the interesting variables

fn_CTR_to_CALL <- function(df_ctrs_orig) {
  
  # we need a contactid for the set of records that happened
  df_ctrs_orig <- df_ctrs_orig %>% 
    mutate(ctr_setid = case_when(is.na(InitialContactId) ~ ContactId, T ~ InitialContactId), .before = 1) %>% 
    mutate(junk = 0, .before = 2)
  
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
    select(-any_of(cols_junk)) %>%
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

  # these are calls
  return(df_ctrs_good)
}

# -------------------------------------------------------------------------
# data transformations
#
# CREATES : dataframe called df_calls which is a dataframe where ...
#           ROWS : represent call records
#           COLS : transformed variables suitable for analysis
#           INPUTS : df_calls_orig created from ctr records

fn_CALL_to_ANALYSIS <- function(df_calls_orig) {
  
  df_calls_transform <- df_calls_orig %>% 
    # set important data types - Timestamps should be POSIX
    # For INBOUND Timestamps Initiation == Connected
    # For OUTBOUND etc, Init is when the operator started some action, and Connected is when the call was answered
    # Sometimes Connected is blank
    mutate(InitiationTimestamp = as.POSIXct(InitiationTimestamp)) %>%    
    mutate(ConnectedToSystemTimestamp = as.POSIXct(ConnectedToSystemTimestamp)) %>%
    mutate(Queue.EnqueueTimestamp = as.POSIXct(Queue.EnqueueTimestamp)) %>% 
    mutate(Queue.DequeueTimestamp = as.POSIXct(Queue.DequeueTimestamp)) %>% 
    mutate(Agent.ConnectedToAgentTimestamp = as.POSIXct(Agent.ConnectedToAgentTimestamp)) %>% 
    mutate(ScheduledTimestamp = as.POSIXct(ScheduledTimestamp)) %>% 
    mutate(TransferCompletedTimestamp = as.POSIXct(TransferCompletedTimestamp)) %>% 
    mutate(Agent.AfterContactWorkStartTimestamp = as.POSIXct(Agent.AfterContactWorkStartTimestamp)) %>% 
    mutate(Agent.AfterContactWorkEndTimestamp = as.POSIXct(Agent.AfterContactWorkEndTimestamp)) %>% 
    mutate(DisconnectTimestamp = as.POSIXct(DisconnectTimestamp)) %>% 
    mutate(LastUpdateTimestamp = as.POSIXct(LastUpdateTimestamp)) %>% 
    
    # we need more granular date and time fields for some timestamps
    # renamed to 'when' family of variables from 18/01/23
    mutate(date_call = as.Date(InitiationTimestamp)) %>% 
    # I know this is a straightforward copy, which may seem pointless from a adat perspective, BUT
    # from an analysis point of view it makes sense to have a 'set' of commonly named variables
    mutate(when_date = date_call) %>%
    mutate(when_week = format(date_call,'%Y-%W')) %>%
    mutate(when_day = format(when_date, '%A')) %>%
    mutate(when_day = factor(when_day, levels = c('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'))) %>%
    mutate(when_month = format(when_date, '%Y-%m')) %>%
    mutate(when_hour = format(InitiationTimestamp, '%H')) %>%
    mutate(when_minute = format(InitiationTimestamp, '%M')) %>%
    mutate(when_second = format(InitiationTimestamp, '%S')) %>%
    
    # times without date
    mutate(tm_sched = format(ScheduledTimestamp, '%H:%M:%S')) %>%
    mutate(tm_init = format(InitiationTimestamp, '%H:%M:%S')) %>%
    mutate(tm_conn = format(ConnectedToSystemTimestamp, format = '%H:%M:%S')) %>%
    mutate(tm_quenq = format(Queue.EnqueueTimestamp, '%H:%M:%S')) %>%
    mutate(tm_qudeq = format(Queue.DequeueTimestamp, '%H:%M:%S')) %>%
    mutate(tm_agcon = format(Agent.ConnectedToAgentTimestamp, '%H:%M:%S')) %>%
    mutate(tm_tranf = format(TransferCompletedTimestamp, '%H:%M:%S')) %>%
    mutate(tm_agwrs = format(Agent.AfterContactWorkStartTimestamp, '%H:%M:%S')) %>%
    mutate(tm_agwre = format(Agent.AfterContactWorkEndTimestamp, '%H:%M:%S')) %>%
    mutate(tm_disc = format(DisconnectTimestamp, '%H:%M:%S')) %>%
    mutate(tm_updat = format(LastUpdateTimestamp, format = '%H:%M:%S')) %>%
    
    # duration in each state
    mutate(dur_init_conn = as.integer(difftime(ConnectedToSystemTimestamp, InitiationTimestamp, unit = 'secs'))) %>%
    mutate(dur_init_que = as.integer(difftime(Queue.EnqueueTimestamp, InitiationTimestamp, unit = 'secs'))) %>%
    mutate(dur_enq_deq = as.integer(difftime(Queue.DequeueTimestamp, Queue.EnqueueTimestamp, unit = 'secs'))) %>%
    mutate(dur_deq_agnt = as.integer(difftime(Agent.ConnectedToAgentTimestamp, Queue.DequeueTimestamp, unit = 'secs'))) %>%
    # time to answer
    mutate(dur_conn = as.integer(difftime(Agent.ConnectedToAgentTimestamp, Queue.DequeueTimestamp, unit = 'secs'))) %>%
    # total customer in-call time
    mutate(dur_call = as.integer(difftime(Agent.AfterContactWorkStartTimestamp, Agent.ConnectedToAgentTimestamp, unit = 'secs'))) %>%
    # interaction time from Connect
    mutate(dur_call_interact = as.integer(Agent.AgentInteractionDuration)) %>%
    # hold time
    mutate(dur_call_hold = as.integer(Agent.CustomerHoldDuration)) %>%
    # mutate(dur_aft = as.integer(difftime(Agent.AfterContactWorkEndTimestamp, Agent.AfterContactWorkStartTimestamp, unit = 'secs'))) %>%
    mutate(dur_aft = as.integer(Agent.AfterContactWorkDuration)) %>%
    mutate(dur_dis_upd = as.integer(difftime(LastUpdateTimestamp, DisconnectTimestamp, unit = 'secs'))) %>%
    mutate(dur_total = as.integer(difftime(LastUpdateTimestamp, InitiationTimestamp, unit = 'secs'))) %>%
    
    # oktaid
    mutate(oktaid = Agent.Username) %>%
    
    # flags
    mutate(flag_weekday = case_when(when_day %in% c('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday') ~ 1, T ~ 0)) %>%
    mutate(flag_queued = case_when(!is.na(tm_quenq) ~ 1, T ~ 0)) %>%
    mutate(flag_answer = case_when(!is.na(tm_agcon) ~ 1, T ~ 0)) %>%
    mutate(flag_inbound = case_when(InitiationMethod == 'INBOUND' ~ 1, T ~ 0)) %>%
    mutate(flag_outbound = case_when(InitiationMethod == 'OUTBOUND' ~ 1, T ~ 0)) %>%
    mutate(flag_other = case_when(!InitiationMethod %in% c('INBOUND','OUTBOUND') ~ 1, T ~ 0)) %>%
    mutate(flag_answerin20 = case_when(flag_answer == 1 & (dur_enq_deq < 21) ~ 1, T ~ 0)) %>%
    mutate(flag_calllonger30 = case_when(dur_call > 30 ~ 1, T ~ 0)) %>%
    
    # phone number they called formatted as a key for reference data
    mutate(system_phone_number = str_replace(SystemEndpoint.Address,'\\+','')) %>%
    
    # keypress information
    mutate(keypress_var = case_when(!is.na(`Attributes.Key Press`) ~ 'Attributes.Key Press',
                                    !is.na(`Attributes.Key press`) ~ 'Attributes.Key press',
                                    !is.na(Attributes.KeyPress) ~ 'Attributes.KeyPress',
                                    !is.na(`Attributes.Key Press (Error)`) ~ 'Attributes.Key Press (Error)',
                                    T ~ 'None')) %>%
    mutate(keypress = coalesce(`Attributes.Key Press`,`Attributes.Key press`,Attributes.KeyPress,`Attributes.Key Press (Error)`)) %>%
    
    # MemberId with the slash delimiter removed
    rename(member_id_aws = Attributes.MemberID) %>% 
    
    identity()
  
  # drop timestamps and contactids as we don't need them any more
  # add variables from various reference tables
  
  cols_drop <- 
    c(df_calls_transform %>% colnames()  %>% str_subset('Timestamp'), 
      df_calls_transform %>% colnames()  %>% str_subset('ContactId'),
      "Agent.Username", "Agent.AfterContactWorkDuration", "Agent.AgentInteractionDuration","Agent.CustomerHoldDuration","Agent.LongestHoldDuration",
      "Attributes.CA Holiday","Attributes.Key Press (Error)","Attributes.Key press","Attributes.KeyPress","Attributes.Key Press")
  
  df_calls <- df_calls_transform %>% 
    mutate(phone.number = str_replace(SystemEndpoint.Address,'\\+','')) %>% 
    left_join(df_services %>% select(phone.number, service), by='phone.number') %>% 
    left_join(df_okta, by='oktaid') %>% 
    select("ctr_setid","leg_count","leg_id",
           "date_call","date_download",
           "InitiationMethod","DisconnectReason",
           "service","phone.number","Channel",
           starts_with("okta"),
           starts_with('flag_'),
           starts_with('call_'),
           starts_with('tm_'),
           starts_with("dur_"),
           "AgentConnectionAttempts","Agent.NumberOfHolds","Agent.RoutingProfile.Name",
           starts_with("Agent.HierarchyGroups"),
           starts_with("Agent"),
           "Attributes.Outcome",
           starts_with("Attributes"),
           everything()) %>% 
    select(-any_of(cols_drop))
  
  strfix <- function(ss) {
    return(str_replace_all(ss, '[\n\r]', ''))
  }
  
  # remove line feeds and carriage returns from text fields
  df_calls <- data.frame(lapply(df_calls, strfix)) %>% 
    mutate(across(starts_with('dur_'), as.integer)) %>% 
    mutate(across(starts_with('flag_'), as.integer)) 
}

