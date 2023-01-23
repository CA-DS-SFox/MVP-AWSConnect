
df_variables <- df_calls %>% 
  colnames %>% 
  as.data.frame() %>% 
  setNames(c('AnalysisVariable'))



