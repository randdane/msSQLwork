# -*- coding: utf-8 -*-
from os import getenv
from datetime import datetime, timedelta
import pymssql  
'''
    Investigate a Batch Settlement that has not proccessed correctly.
    A list will be returned with the first element as the Batch number.
    All following items in the list will consist of a four item tuple.
    Each tuple will consist of the TransactionID, token, card type,
    and ProviderID.
'''
def add_x_minutes(tran_timestamp, time_minutes = 3):
    '''
        Convert SQL timestamp to Python datetime, add about 3 minutes
        to it, and return it back as a SQL timestamp.
    '''
    datetime_ts = datetime.strptime(tran_timestamp[:-4],
                  '%Y-%m-%d %H:%M:%S') # milliseconds are not needed
    incremented_ts = datetime_ts + timedelta(minutes = time_minutes)
    return incremented_ts.strftime('%Y-%m-%d %H:%M:%S')
    
# Data used for investigation will be recorded in a new file
now = datetime.now()
new_file = now.strftime('%Y%m%d-%H%M%S') + '-Settle_Investigate.txt'


server = '127.0.0.1' # local server used for testing
conn = pymssql.connect(server)
cursor = conn.cursor()

batches_invalid = []
trans_invalid = []

# query outstanding batches except for today's
get_batch_invalid = '''
    SELECT ID,BatchNumber,BatchDate,BatchSettledDateTime,
    SettlementSubmitted,SettlementResponse 
    FROM KCXPPN.dbo.SettlementBatch 
    WHERE (BatchSettledDateTime IS NULL)
    AND (BatchDate <> CONVERT(date, GETDATE())) '''

# query transactions in selected batch
get_trans_invalid = '''
    SELECT ID, SalesDate, CardType, PANLastFour, Token, ProviderID
    FROM KCXPPN.dbo.PaymentTransaction
    WHERE SettlementBatchID = 'x' AND HostResponse = 'APPROVED' 
    AND (TransactionCode IN ('11', '13', '16', '21')) 
    ORDER BY ProviderID '''

# query MessagingEvent table for token only response
get_token_valid = '''
    SELECT [Message],[LoggedDateTime]
    FROM KCXPPN.dbo.MessagingEvent
    WHERE LoggedDateTime BETWEEN '{}' AND '{}'
    ORDER BY LoggedDateTime '''

with pymssql.connect(server) as conn:
    
    with conn.cursor(as_dict = True) as cursor:
        
        cursor.execute(get_batch_invalid)
        for row in cursor:
            
            batches_invalid.append((row['ID'], row['BatchNumber']))
            if row[5] = 5: # Batch is invalid and must investigate
                ''' Get transactions from invalid Batch '''
                cursor.execute(get_trans_invalid.format(row[0]))
                for row in cursor:
                                                
                    if row[3] == 'PLC':
                        pass
                        # transaction must be moved to a PLC only batch
                            
                    elif row[9] == None:
                            
                            # get ID, SalesDate, CardType, PANLastFour, Token, ProviderID
                            trans_invalid.append([row])
                            
                    for item in trans_invalid:      # search for missing tokens
                        
                        cursor.execute(get_token_valid.format(''' insert time frame here ''')
                        for row in cursor:
                                       
                            if 'token-only-response' in row:
                                pass
                                # parse string with RegEx to get token
                                
# Not done yet
                 
# Write data to file
with open(new_file, 'a+') as f:
    for item in batches_invalid:
        f.write('ID = %d, BatchNumber = %s \n' % (row['ID'], row['BatchNumber']))
        for sub_item in item:
            f.write('ID = %d,OriginalTranID = %s,SalesDate = %s,CardType = %s,\
            PANLastFour = %d,Token = %d,ProviderID = %d,LocalDateTime = %s,\
            HostResponse = %s \n' % (row['ID'], row['OriginalTranID'],
            row['SalesDate'], row['CardType'], row['PANLastFour'], row['Token'],
            row['ProviderID'], row['LocalDateTime'], row['HostResponse']))
            
