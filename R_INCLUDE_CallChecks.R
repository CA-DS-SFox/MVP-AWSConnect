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

# check Attributes.MemberID is valid
df_calls %>% 
  count(Attributes.MemberID) %>% 
  rename(member_aws = Attributes.MemberID) %>% 
  left_join(df_mbr, by = 'member_aws') %>% 
  select(member_aws, member_name, n) %>% 
  filter(is.na(member_name))
