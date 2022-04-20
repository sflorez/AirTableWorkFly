from utils import getCSVData, Bases
from pprint import pprint
from pyairtable import Table
import airTableApi

data = getCSVData(filepath='av_contacts.csv', withCodec='utf-8-sig', delimiter=',')

pprint(len(data))

base = Bases.get('CRM')

contactsTable = airTableApi.getTable(baseId=base, tableName='Contacts')

allContacts = contactsTable.all()

pprint(len(allContacts))

recordsToCreate = []

