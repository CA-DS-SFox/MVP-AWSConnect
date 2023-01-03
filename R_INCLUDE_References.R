

# reference tables

# -------------------------------------------------------------------------

source_okta <- 'O:/Technology/Data Service - Advisers/data-raw/historic/2022-12-18_AllUsersbyMRB_Vols.csv'
df_okta <- read_csv(source_okta, col_types = cols(.default='c'))

df_okta <- df_okta %>% 
  mutate(okta.name = paste(firstName, lastName)) %>% 
  mutate(okta.MBR = office) %>% 
  mutate(okta.type = case_when(volunteer == 'yes' ~ 'Volunteer',
                               volunteer == 'no' ~ 'Staff',
                               T ~ 'Unknown')) %>% 
  select(oktaid, okta.name, okta.MBR, okta.type) %>% 
  group_by(oktaid) %>% 
  filter(row_number() == 1) %>% 
  ungroup()

# -------------------------------------------------------------------------

df_services <- tibble::tribble(~service, ~phone.number, ~Description,
                               "IT Service Desk",442034679897, NA,
                               "Consumer Operations",443005000922, NA,
                               "Consumer Operations",442034679560,"Pre-porting temp number",
                               "Network Support",443451202035, NA,
                               "Network Support",448451202035, NA,
                               "Network Support",442034679563,"Pre-porting temp number",
                               "MAPSDAP",442036950102,"Pre-porting temp number",
                               "MAPSDAP",448081788398, NA,
                               "MAPSDAP",442036950124,"From Adviceline Queue IVR",
                               "MAPSDAP",442036086687, "Returned calls message",
                               "Birmingham Debt",442038300097, NA,
                               "Help To Claim",448081897199,"England",
                               "Help To Claim",448081897129,"Wales",
                               "Help To Claim",442038300543,"BSL",
                               "Help To Claim",442034672252, "Advicelink Landing Pad",
                               "Help To Claim",442038300606, "Adviceline Landing Pad",
                               "Help Through Hardship",448081788098, NA,
                               "Help Through Hardship",442034676920,"Voicemail",
                               "EU Citizens Rights",443300544903, NA,
                               "Witness Service",443300544685, "English & Staff / Partners",
                               "Witness Service",443300544917,"Welsh",
                               "Witness Service",443300544920,"BSL",
                               "EDF",443003300519,"Inbound",
                               "EDF",448081788049, "Outbound",
                               "EDF",442034679579,"Pre-porting temp number",
                               "EDF",448081566666, "Main public number",
                               "Client Services",442034679590,"Pre-porting temp number",
                               "Client Services",443000231900, NA,
                               "Macmillan",442034679542,"Pre-porting temp number",
                               "Macmillan",443003302120, NA,
                               "Healthwatch Isle of Wight",442038300570, NA,
                               "Immigration Advisory",442034679789, NA,
                               "Dudley Empowerment",442034679793, NA,
                               "Pension Wise",	443300544866,	"Inbound Calls",
                               "Consumer",	448081897277,	"Inbound English",
                               "Internal Users",	443003302152,	"Accenture System Checks",
                               "Internal Users",	442038301603,	"Phones Team Live Proving Line",
                               "Witness Service",	448081897255,	"English : Staff/Partners and BSL : Outbound") %>% 
  mutate(phone.number = as.character(phone.number))
