# -*- coding: utf-8 -*-
from os import getenv
from datetime import datetime, timedelta
from pprint import pprint
import pymssql
import re
'''
    Investigate a Batch Settlement that has not proccessed correctly.
    A list of dictionaries containing all pertanent Batch info will
    be created. A 'Transactions' item will be added and its value will
    be a list of invalid transactions that fall under that Batch.
    Each transaction will be a dictionary will all required info.
'''
def add_x_minutes(tran_ts, time_minutes = 3):
    '''
        Add three minutes to timestamp for search purposes.
    '''
    return tran_ts + timedelta(minutes = time_minutes)

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
    WHERE (LoggedDateTime BETWEEN '{0}' AND '{1}')
    AND (Message LIKE 'Event : Token only payload:%')
    ORDER BY LoggedDateTime '''
    
# update token with recovered value
update_token_valid = '''
    UPDATE KCXPPN.dbo.PaymentTransaction SET Token = '{Token}' WHERE ID = '{ID}'
    UPDATE KCXPPN.dbo.PaymentTransaction SET ProviderID = '{ProviderID}' WHERE ID = '{ID}' '''

# dictionary of Card Types matched to their ProviderID
card_to_ID = {'MC' : '001', 'VISA' : '002', 'AMEX' : '006', 'DISC' : '003'}

# update batch for re-settlement
update_batch = '''
    Update KCXPPN.dbo.SettlementBatch 
    SET BatchSettledDateTime = NULL, SettlementSubmitted = '0', SettlementResponse = NULL
    WHERE ID = '{0}' '''

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
    cursor.execute(get_tran_invalid.format(item['ID']))
    tran_item = cursor.fetchone()
    while tran_item:
        item['Transactions'].append(tran_item)
        tran_item = cursor.fetchone()

for item in batch_invalid: 
    for subitem in item['Transactions']:
        sql_ts = subitem['SalesDate']
        cursor.execute(get_token_valid.format\
        (str(sql_ts)[:-7], str(add_x_minutes(sql_ts))[:-7]))
        token_item = cursor.fetchone()
        while token_item:
            if 'Event : Token only payload:' in token_item['Message']:
                tag07data = token_item['Message'].split('|')
                subitem['Token'] = re.search(r'(07){1}([0-9]*)', tag07data[-2]).group(2)

            token_item = cursor.fetchone()
        
        subitem['PANLastFour'] = card_to_ID[subitem['CardType']]

# insert token and ProviderID into Transaction and update record
for item in batch_invalid: 
    for subitem in item['Transactions']:
        pass # cursor.execute(update_token_valid.format(**subitem))

# reset each batch for auto settlement
if batch_invalid:
    for item in batch_invalid:
        cursor.execute(update_batch.format(item['ID']))

conn.close()

# Write all data involved in batch investigation to new file with timestamp
now = datetime.now()
new_file = now.strftime('%Y%m%d-%H%M%S') + '-Settle_Investigate.txt'
with open(new_file, 'w') as f:
    f.write(str(batch_invalid))
