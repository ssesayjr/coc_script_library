import os 
import datetime 
import pandas as pd 
import time 
import sys
import json

from parsons import Table, S3, Redshift, utilities, logger 
from canalespy import setup_environment #specifc to TMC
import xml.etree.ElementTree as ET
import numpy as np


def main():
    #setup_environment()
    setup_environment()


    #set begin time to see how long the script takes to run
    start = time.time()

    #create an instance of S3 and Redshift
    rs = Redshift()
    s3 = S3()

    bucket = os.environ['BUCKET']
    keys = s3.list_keys(bucket)
    files = keys.keys()
    

    #grab keys from bucket
    #s3_key_infos = s3.list_keys(bucket, suffix='.xlm') 

    #s3_keys_tuples = sorted(s3_key_infos.items(), reverse=True, key=lambda dictitem: dictitem[1]['LastModified'])
    #s3_keys = [item[0] for item in s3_keys_tuples]
    #last_modified = s3_keys[0] #grabs only the key of the last modified file


    timestamp = datetime.datetime.now().strftime("%Y%m%d") #timestamp used in log table 


    def setup_environment(redshift_parameter="REDSHIFT", aws_parameter="AWS"):
        """
        Sets up environment variables needed for various common services used by our scripts.
        Call this at the beginning of your script.
        `Args:`
            redshift_parameter: str
                Name of the Civis script parameter holding Redshift credentials. This parameter
                should be of type "database (2 dropdown)" in Civis.
            aws_parameter: str
            Name of the Civis script parameter holding AWS credentials.
            copper_parameter: str
            Name of the Copper script parameter holding Copper user email and API key
            google_sheets_parameter: str
                Name of the Google Sheets script parameter holding a base64-encoded Google
                credentials dict
        """

        user_table = f'{mem_code}_dmarc.users_{org_type}'

        if not rs.table_exists(user_table): #if the table doesn't exist create it
            rs.query(f"create table {user_table} (org_name varchar(1024), email varchar(1024), report_ID varchar(1024), date_range_begin timestamp,date_range_end timestamp, domain varchar(1024), adkim varchar(1024), aspf varchar(1024), p varchar(1024), sp timestamp, pct varchar(1024), source_ip varchar(1024), count varchar(1024), disposition varchar(1024), dkim varchar(1024), spf varchar(1024));")

            
    def file_To_Table(file):
      
        for column in file.data:
            column_data = str(column)
            
            if '<org_name>' in column_data:
                start = column_data.find('<org_name>') + len('<org_name>')
                end = column_data.find('</org_name>')
                org_substring = column_data[start:end]

            if '<email>' in column_data:
                start = column_data.find('<email>') + len('<email>')
                end = column_data.find('</email>')
                email_substring = column_data[start:end]

            if '<report_id>' in column_data:
                start = column_data.find('<report_id>') + len('<report_id>')
                end = column_data.find('</report_id>')
                id_substring = column_data[start:end]
      
            if '<begin>' in column_data:
                start = column_data.find('<begin>') + len('<begin>')
                end = column_data.find('</begin>')
                begin_substring = column_data[start:end]
                
            if '<end>' in column_data:
                start = column_data.find('<end>') + len('<end>')
                end = column_data.find('</end>')
                end_substring = column_data[start:end]
                
                
            if '<domain>' in column_data:
                start = column_data.find('<domain>') + len('<domain>')
                end = column_data.find('</domain>')
                domain_substring = column_data[start:end]                
    
    
            if '<adkim>' in column_data:
                start = column_data.find('<adkim>') + len('<adkim>')
                end = column_data.find('</adkim>')
                adkim_substring = column_data[start:end]    
    
    
            if '<aspf>' in column_data:
                start = column_data.find('<aspf>') + len('<aspf>')
                end = column_data.find('</aspf>')
                aspf_substring = column_data[start:end]    
    
            if '<p>' in column_data:
                start = column_data.find('<p>') + len('<p>')
                end = column_data.find('</p>')
                p_substring = column_data[start:end]    
    
    
            if '<sp>' in column_data:
                start = column_data.find('<sp>') + len('<sp>')
                end = column_data.find('</sp>')
                sp_substring = column_data[start:end]    
    
    
            if '<pct>' in column_data:
                start = column_data.find('<pct>') + len('<pct>')
                end = column_data.find('</pct>')
                pct_substring = column_data[start:end]    

            if '<source_ip>' in column_data:
                start = column_data.find('<source_ip>') + len('<source_ip>')
                end = column_data.find('</source_ip>')
                ip_substring = column_data[start:end]    
    
            if '<dkim>' in column_data:
                start = column_data.find('<dkim>') + len('<dkim>')
                end = column_data.find('</dkim>')
                dkim_substring = column_data[start:end]    
  
            if '<disposition>' in column_data:
                start = column_data.find('<disposition>') + len('<disposition>')
                end = column_data.find('</disposition>')
                disposition_substring = column_data[start:end]   

            if '<spf>' in column_data:
                start = column_data.find('<spf>') + len('<spf>')
                end = column_data.find('</spf>')
                spf_substring = column_data[start:end]    
        
            if '<result>' in column_data:
                start = column_data.find('<result>') + len('<result>')
                end = column_data.find('</result>')
                result_substring = column_data[start:end]    
    
        df = pd.DataFrame({'Org Name':[org_substring],
                        'Email': [email_substring],
                        'Report ID': [id_substring],
                        'Date Range Begin': [begin_substring],
                        'Date Range End': [end_substring],
                        'Domain': [domain_substring],
                        'ADKIM': [adkim_substring],
                        'ASPF': [aspf_substring],
                        'P': [p_substring],
                        'SP': [sp_substring],
                        'PCT': [pct_substring],
                        'Source IP': [ip_substring],
                        'Disposition': [disposition_substring],
                        'DKIM': [dkim_substring],
                        'SPF': [spf_substring]})
    
        parsons_table = Table.from_dataframe(df)
        return parsons_table
    
    
    if len(keys) == 0:
        print ("No files to sync today!")
    else:
        for x in files:
            from parsons import Table
            file = s3.get_file(bucket, x)
            file = Table.from_csv(file)
            
                
             #TODO: Table undeifned, may need to import from parsons?
            final_table = file_To_Table(file)
            table_name = f"schema.{S3_TEMP_BUCKET}"
            try:
                final_table.to_redshift(table_name, if_exists='truncate')
            except Exception:
                final_table.to_redshift(table_name, if_exists='drop')
            utilities.files.close_temp_file(file)

    
    #final_table.to_redshift(user_table, specifycols=True, if_exists='truncate')

if __name__ == '__main__':
    main()
