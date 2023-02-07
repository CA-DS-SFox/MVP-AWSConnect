
#%%
from datetime import datetime
import boto3 as b3
import pandas as pd
import gspread

#%%
def fn_writeheartbeat(out_path):
   file_name = f'heartbeat_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.txt'
   file_out = f'{out_path}/{file_name}'
   print(file_out)
   
   f= open(file_out,"w+")
   f.write(f'testing testing {file_out}')
   f.close()
   return(file_name)

def fn_gets3tokens(key_path, key_file):
    df_secret = pd.read_csv(f'{key_path}/{key_file}')
    s3_key = df_secret[(df_secret.Type == 'S3_KEY')]['Data'].to_list()[0]
    s3_secret = df_secret[(df_secret.Type == 'S3_SECRET')]['Data'].to_list()[0]
    return s3_key, s3_secret

def fn_writetos3(bucket_name, s3_key, s3_secret, object_name_in, object_name_out):
    session = b3.Session(
      aws_access_key_id = s3_key,
      aws_secret_access_key = s3_secret
    )
    
    s3 = session.resource('s3')
    object = s3.Object(bucket_name, object_name_out)
    
    result = object.put(Body=open(object_name_in, 'rb'))
    res = result.get('ResponseMetadata')

    if res.get('HTTPStatusCode') == 200:
        print('File Uploaded Successfully')
    else:
        print('File Not Uploaded')

def fn_gsheet_to_df(name_sheet, 
                    name_tab, 
                    header_row = 1, 
                    data_starts = 0,
                    print_indent = None, 
                    verbose = False):

    # open the googledoc
    try:
        # you need the credentials.json in C:\Users\you\AppData\Roaming\gspread
        # for this to authenticate
        gc = gspread.oauth()
        # open the google-document
        sheet = gc.open(name_sheet)

        # open the specific worksheet
        worksheet = sheet.worksheet(name_tab)

    except Exception as ex:
        print ("Exception type - {0} - occurred. Arguments: {1!r}".format(type(ex).__name__, ex.args))

    # print indent ----------------------------------------------------------------------------
    indent = ''
    if (isinstance(print_indent, int)):
        indent = ' ' * print_indent

    # report the size of the worksheet---------------------------------------------------------
    ncol, nrow = worksheet.col_count, worksheet.row_count
    print(f"{indent}Worksheet dimensions : Cols {ncol}, Rows {nrow}, Cells {ncol * nrow}")

    # get all the cells
    all_cells = worksheet.get_all_values()

    # get cell list dimensions
    ccol, crow = len(all_cells[0]), len(all_cells)
    print(f"{indent}Cell list dimensions : Cols {ccol}, Rows {crow}, Cells {ccol * crow}")

    # make a dataframe -----------------------------------------------------------------------
    df = pd.DataFrame(all_cells)

    # headers --------------------------------------------------------------------------------
    if isinstance(header_row, str):
        header_row = int(header_row)

    if isinstance(data_starts, str):
        data_starts = int(data_starts)

    # we might want to just return the dataframe as-is, so if header_row == 0 don't do anything
    if header_row > 0:
        headers = df.iloc[header_row - 1]
    if verbose:
        print(f'{indent} ... No header row')

    # for python, if sheet row is 1 needs to be dataframe row 0
    if verbose:
        print(f'{indent} ... Header row is {header_row}, which is dataframe row {header_row - 1}')
        print(f'{indent} ... Column names will be : {headers.to_list()}')

    # get the actual data which starts in the row after the header row, so 
    # given the zero based dataframe row numbering, that's the same thing
    # as the header_row arguement which is 1 based
    if header_row > 0:
        if data_starts == 0:
            data_starts = header_row + 1

    df = pd.DataFrame(df.values[data_starts:], columns = headers)

    # report information ---------------------------------------------------------------------
    nrow, ncol = df.shape
    print(f"{indent}Dataframe dimensions : Cols {ncol}, Rows {nrow}, Cells {ncol * nrow}")

    return(df)



#%%
if __name__ == '__main__':
    
    # create a heartbeat file on the G: drive
    file_path = 'G:/Shared drives/CA - Interim Connect Report Log Files & Guidance/Interim Reports/Testing'
    file_name = fn_writeheartbeat(file_path)
    
    # get s3 keys
    s3_key, s3_secret = fn_gets3tokens('G:/My Drive/RAP', 'secret.txt')
    
    # my keys work for this bucket
    # fn_writetos3('network-data-service-development', s3_key, s3_secret, f'{file_path}/{file_name}', file_name)
    
    # but this one gives me an access denied error
    # fn_writetos3('suzanne-101', s3_key, s3_secret, 'G:/My Drive/RAP/TUG.jpg', 'TUG.jpg')

    df = fn_gsheet_to_df("CTR Map","CTR Records")

#%%
    print(df.head(5))

# %%
