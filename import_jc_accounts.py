import traceback
import csv
import airTableApi
import codecs
from utils import Bases
from utils import getCSVData
from pprint import pprint

base = Bases.get('CRM')
tableName = 'All Accounts'

JCData = getCSVData(filepath='AllJetClubAccounts.csv', withCodec=None, delimiter=',')

accountsTable = airTableApi.getTable(baseId=base, tableName=tableName)

# pprint(JCData)
allAccounts = accountsTable.all()

recordsToUpdate = []
recordsToCreate = []
for data in JCData:
    # check if we need to update a record
    accountRecord = [el for el in allAccounts if el['fields'].get('account_name') == data.get('account_name')]
    if accountRecord:
        recordsToUpdate.append({'id' : accountRecord[0].get('id'), 'fields' : {'work_phone': data.get('work_phone'), 
                                                                               'email': data.get('email'),
                                                                               'jet_club_customer_id': data.get('jet_club_customer_id'),
                                                                               'av_customer_id': data.get('av_customer_id'),
                                                                               'primary_address_id': data.get('primary_address_id'),
                                                                               'account_name' : data.get('account_name'), 
                                                                               'account_type' : data.get('account_type'), 
                                                                               'entity_type': data.get('entity_type'), 
                                                                               'intacct_id': data.get('intacct_id'), 
                                                                               'sales_rep_id': data.get('sales_rep_id'), 
                                                                               'updated_at': data.get('updated_at')}})
    else:
        recordsToCreate.append(data)

accountsTable.batch_update(recordsToUpdate)
accountsTable.batch_create(recordsToCreate)