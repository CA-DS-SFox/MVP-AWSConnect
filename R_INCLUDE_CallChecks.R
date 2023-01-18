# CHECK : 
#   Reference tables define all versions of source data
#
#

# -------------------------------------------------------------------------

# service reference is missing, or assumed from the ServiceName
df_calls %>% 
  filter(is.na(service) | str_detect(service, 'ASSUME')) %>% 
  group_by(phone.number, service, Attributes.ServiceName) %>% 
  summarise(date_from = min(when_date),
            no_of_calls = n())


# check oktaid is distinct
df_okta %>% 
  add_count(oktaid) %>% 
  filter(n > 1)
