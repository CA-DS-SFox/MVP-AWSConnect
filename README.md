# MVP-AWSConnect
R scripts exploring transforms needed to create reporting analysis dataset from flattened JSON data - 2023/01/03

## R-101_GetCSVData.R
- Accumualates CSV data from daily exports created from Jon's flattening script that is run manually every morning
- Saves as a parquet file of CTR data 

## R_102_Analyze.R
- Creates a grouping variable (ctr_setid) so multi-leg calls with multiple CTRs can be identified
- R_INCLUDE_CallRecordDef.R : Identifies CTRs which are duplicate or artefacts of the system and which should be junked
- R_INCLUDE_CallTransforms.R : the transforms that create the data in an analysable structure
