
questions <- df_ctrs_clean %>% colnames() %>% str_subset('uestion')

df_questions <- df_ctrs_clean %>% 
  select(ctr_setid, call_date, leg_count, leg_id, all_of(questions)) %>% 
  identity()

