from utils import getCSVData
from pprint import pprint

atContacts = getCSVData('Contacts-contacts (personal).csv', withCodec='utf-8-sig', delimiter=',')
brokerInfo = getCSVData('AccountBrokerInformationCSV.csv', withCodec='utf-8-sig', delimiter=',')
pgContacts = getCSVData('airtable_contacts.csv', withCodec='utf-8-sig', delimiter=',')

contactNames = [el.get('Name') for el in atContacts]
brokerContactNames = [el.get('BrokerFirstName').strip() + " " + el.get('BrokerLastName').strip() for el in brokerInfo]
pgContactNames = [el.get('first_name').strip() + " " + el.get('last_name').strip() for el in pgContacts]


uniqueBrokerContacts = set(brokerContactNames)
uniquePgContacts = set(pgContactNames)
uniqueAtContacts = set(contactNames)

pprint(len(uniqueBrokerContacts))
pprint(len(uniquePgContacts))
pprint(len(uniqueAtContacts))

