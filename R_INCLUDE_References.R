

# reference tables

mbr_source <- 'Member + Location sheet (Salesforce data)'
mbr_google <- 'https://docs.google.com/spreadsheets/d/1SIFJPCA8duV7qFnq6u1eq8o8tHEXMMFGCVa4Ea6nCWc/edit#gid=2032453984'
mbr_sheet <- 'Member List (1)'

df_mbr <- googlesheets4::read_sheet(mbr_google, sheet = mbr_sheet) %>%
  select(member_number = Membership_Number__c, member_name = Name) %>% 
  mutate(mbr_source = paste0('Googlesheet : ', mbr_source)) %>% 
  mutate(mbr_extract_date = Sys.Date()) %>% 
  mutate(member_aws = str_replace(member_number, '/','')) %>% 
  select(mbr_extract_date, mbr_source, member_number, member_aws, member_name)
  
# -------------------------------------------------------------------------

#source_okta <- 'O:/Technology/Data Service - Advisers/data-raw/historic/2022-12-18_AllUsersbyMRB_Vols.csv'
source_okta <- 'O:/Technology/MVP-OktaExports/data/reporting_oktaadvisers.csv'
df_okta <- read_csv(source_okta, col_types = cols(.default='c'))

df_okta <- df_okta %>% 
  rename(oktaid = okta_id) %>% 
  rename(okta_name = advisername) %>% 
  rename(okta_member_name = office) %>% 
  rename(okta_member_number = member_id) %>% 
  select(oktaid, okta_name, okta_member_number, okta_member_name) %>% 
  ungroup()

# -------------------------------------------------------------------------

df_services <- tibble::tribble(~date_from, ~service, ~phone.number, ~Description, 
                               "2023-01-01", "IT Service Desk", 442034679897, NA,
                               "2023-01-01", "Consumer Operations",443005000922, NA,
                               "2023-01-01", "Consumer Operations",442034679560,"Pre-porting temp number",
                               
                               "2023-01-18", "ASSUME - Consumer Service", 443300544695, NA,
                               "2023-01-18", "ASSUME - Consumer Service", 443300544925, NA,
                               "2023-01-18", "ASSUME - Consumer Service", 448081788171, NA,
                               "2023-01-18", "ASSUME - Consumer Service", 443300544934, "Welsh",
                               "2023-01-18", "ASSUME - Consumer Service", 448081897265, "Welsh",
                               "2023-01-18", "ASSUME - Consumer Service", 448081897281, "start scam",
                               
                               "2023-01-01", "Network Support",443451202035, NA,
                               "2023-01-01", "Network Support",448451202035, NA,
                               "2023-01-01", "Network Support",442034679563,"Pre-porting temp number",
                               "2023-01-01", "MAPSDAP",442036950102,"Pre-porting temp number",
                               "2023-01-01", "MAPSDAP",448081788398, NA,
                               "2023-01-01", "MAPSDAP",442036950124,"From Adviceline Queue IVR",
                               "2023-01-01", "MAPSDAP",442036086687, "Returned calls message",
                               "2023-01-01", "Birmingham Debt",442038300097, NA,
                               "2023-01-01", "Help To Claim",448081897199,"England",
                               "2023-01-01", "Help To Claim",448081897129,"Wales",
                               "2023-01-01", "Help To Claim",442038300543,"BSL",
                               "2023-01-01", "Help To Claim",442034672252, "Advicelink Landing Pad",
                               "2023-01-01", "Help To Claim",442038300606, "Adviceline Landing Pad",
                               "2023-01-01", "Help Through Hardship",448081788098, NA,
                               "2023-01-01", "Help Through Hardship",442034676920,"Voicemail",
                               "2023-01-01", "EU Citizens Rights",443300544903, NA,
                               "2023-01-01", "Witness Service",443300544685, "English & Staff / Partners",
                               "2023-01-01", "Witness Service",443300544917,"Welsh",
                               "2023-01-01", "Witness Service",443300544920,"BSL",
                               "2023-01-01", "EDF",443003300519,"Inbound",
                               "2023-01-01", "EDF",448081788049, "Outbound",
                               "2023-01-01", "EDF",442034679579,"Pre-porting temp number",
                               "2023-01-01", "EDF",448081566666, "Main public number",
                               "2023-01-01", "Client Services",442034679590,"Pre-porting temp number",
                               "2023-01-01", "Client Services",443000231900, NA,
                               "2023-01-01", "Macmillan",442034679542,"Pre-porting temp number",
                               "2023-01-01", "Macmillan",443003302120, NA,
                               "2023-01-01", "Healthwatch Isle of Wight",442038300570, NA,
                               "2023-01-01", "Immigration Advisory",442034679789, NA,
                               "2023-01-01", "Dudley Empowerment",442034679793, NA,
                               "2023-01-01", "Pension Wise",	443300544866,	"Inbound Calls",
                               "2023-01-01", "Consumer",	448081897277,	"Inbound English",
                               "2023-01-01", "Internal Users",	443003302152,	"Accenture System Checks",
                               "2023-01-01", "Internal Users",	442038301603,	"Phones Team Live Proving Line",
                               "2023-01-01", "Witness Service",	448081897255,	"English : Staff/Partners and BSL : Outbound") %>% 
  mutate(phone.number = as.character(phone.number))
