# -*- coding: utf-8 -*-
from os import getenv
from datetime import datetime, timedelta
import pymssql
import re
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

# query outstanding batches except for today's
get_batch_invalid = '''
    DECLARE @CurrentDay as date
    SET @CurrentDay = CONVERT(date, GETDATE())
    SELECT ID,BatchNumber,BatchDate,BatchSettledDateTime,SettlementSubmitted,SettlementResponse 
    FROM KCXPPN.dbo.SettlementBatch 
    WHERE (BatchSettledDateTime IS NULL) and (BatchDate <> @CurrentDay)
    ORDER BY BatchDate '''
# query transactions in selected batch
get_tran_invalid = '''
    SELECT ID, SalesDate, CardType, PANLastFour, Token, ProviderID
    FROM KCXPPN.dbo.PaymentTransaction
    WHERE SettlementBatchID = '{0}' AND HostResponse = 'APPROVED' 
    AND (TransactionCode IN ('11', '13', '16', '21')) 
    ORDER BY ProviderID '''
# query MessagingEvent table for token only response
get_token_valid = '''
    SELECT Message, LoggedDateTime
    FROM KCXPPN.dbo.MessagingEvent
    WHERE LoggedDateTime BETWEEN '{0}' AND '{1}'
    ORDER BY LoggedDateTime '''

server = '127.0.0.1'
conn = pymssql.connect(server)
cursor = conn.cursor(as_dict = True)
cursor.execute(get_batch_invalid)
batch_item = cursor.fetchone()
batch_invalid = []

''' Create list of batches (each a dict) '''
while batch_item:
    batch_item['Transactions'] = []
    batch_invalid.append(batch_item)
    batch_item = cursor.fetchone()

''' Create list of transactions inside batch (each a dict) '''
for item in batch_invalid: 
    cursor.execute(get_tran_invalid.format\
    (batch_invalid[0]['ID'])
    tran_item = cursor.fetchone()
    while tran_item:
        batch_invalid[item]['Transactions'].append(tran_item)
        tran_item = cursor.fetchone()

    for subitem in batch_invalid[item]['Transactions']:
        sql_ts = batch_invalid[item]['Transactions'][subitem]['SalesDate']
        cursor.execute(get_token_valid.format\
        (sql_ts, add_x_minutes(sql_ts)))
        token_item = cursor.fetchone()
        while token_item:
            if re.match('Token only payload', token_item['Message'])
                batch_invalid[item]['Transactions'][subitem]['Token']
                = re.search('\d{13,20}' +
                batch_invalid[item]['Transactions'][subitem]['PANLastFour']
                           
# Trying to get '1186601810474155' out of token response:
''' Event : Token only payload: 0|1CAPPROVAL|20|20|20|20|20|20|20|20|1C
1107|1C|1C|1C|1C|1C|1C|1C|1C|1BA0SP021070161186601810474155|1B '''
# Where 4155 is the 'last four' of the card number
# and the '16' in 7016 refers to the length of the token
''' Use 'last four' and search about 6 numbers to the left,
look for '70' and then get the length of the token next to it
use recursive function to compare length to token guess so far '''
                            
                           

                   
'''
cursor.execute('SELECT * FROM persons WHERE salesrep=%s', 'John Doe')
row = cursor.fetchone()
while row:
    print("ID=%d, Name=%s" % (row[0], row[1]))
    row = cursor.fetchone()

conn.close()
'''
print(batch_invalid)
