from dataclasses import fields

from requests import head
import airTableApi
import csv
from utils import Bases, getCSVData, writeCSVFile
from pprint import pprint

crm = Bases.get('CRM')
contactsTable = airTableApi.getTable(crm, 'Contacts')

data = getCSVData('recent_av_contacts.csv', withCodec='utf-8-sig', delimiter=',')

allContacts = contactsTable.all(fields=['primary_id', 'avianis_primary_id'])

def compareIds(firstId, secondId, compareId):
    if (firstId == compareId):
        return True
    elif(secondId == compareId):
        return True
    else:
        return False


recordsLeftToCreate = []
recordsThatExist = []
for row in data:
    matchingId = [el for el in allContacts if compareIds(el['fields'].get('primary_id'), el['fields'].get('avianis_primary_id'), row.get('id'))]
    if matchingId:
        recordsThatExist.append(row)
    else:
        recordsLeftToCreate.append(row)

pprint(len(recordsLeftToCreate))
pprint(len(recordsThatExist))

header = ['id',	'avianis_id', 'first_name',	'middle_name', 'last_name', 'id_information_id',	'primary_address_id',	'customer_id',	'updated_at',	'last_updated_airtable',	'is_archived',	'is_primary',	'citizenship',	'birth_city',	'birth_country',	'dob',	'gender',	'nationality',	'birth_state',	'weight',	'residency']
writeCSVFile(header, recordsLeftToCreate, 'utf-8', '')