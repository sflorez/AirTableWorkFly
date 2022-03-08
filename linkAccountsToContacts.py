import airTableApi
from utils import Bases, TestBases
from utils import getCSVData
from pprint import pprint

base = TestBases.get('CRM')
accountsTable = airTableApi.getTable(baseId=base, tableName='All Accounts')
contactsTable = airTableApi.getTable(baseId=base, tableName='Contacts')

roles = {1 : 'primary',
         2 : 'authorizer',
         3 : 'passenger'}

linkData = getCSVData(filepath='customer_contact.csv', withCodec=None, delimiter=',')

# pprint(linkData)

jetClubAccounts = accountsTable.all(view='Jet Club Accounts')
allContacts = contactsTable.all()
recordsToUpdate = []

def getContactRecord(contactId):
    contactRecord = [el for el in allContacts if el['fields'].get('primary_id') == contactId]
    # pprint(contactId)
    if contactRecord:
        return contactRecord[0]
    else:
        return None

for account in jetClubAccounts:
    primaryToLink = []
    contactsToLink = []
    authorizersToLink = []
    recordToUpdate = {}
    contactLinkData = [el for el in linkData if el.get('customer_id') == account['fields'].get('customer_id')]
    primary = [el.get('contact_id') for el in contactLinkData if roles.get(int(el.get('role_id'))) == 'primary']
    contacts = [el.get('contact_id') for el in contactLinkData if roles.get(int(el.get('role_id'))) == 'passenger']
    authorizers = [el.get('contact_id') for el in contactLinkData if roles.get(int(el.get('role_id'))) == 'authorizer']
    if primary:
        #lookup primary contact info
        print('primary stuff')
        possibleLink = getContactRecord(primary[0])
        if possibleLink:
            primaryToLink.append(possibleLink.get('id'))
            recordToUpdate['primary'] = primaryToLink
    if contacts:
        #lookup contacts infos
        print('contact stuff')
        for contact in contacts:
            possibleLink = getContactRecord(contact)
            if possibleLink:
                contactsToLink.append(possibleLink.get('id'))
        dedupe = set(contactsToLink)
        recordToUpdate['contacts'] = list(dedupe)
    if authorizers:
        #lookup auth bois
        print('auth stuff')
        for authorizer in authorizers:
            possibleLink = getContactRecord(authorizer)
            if possibleLink:
                authorizersToLink.append(possibleLink.get('id'))
        dedupe = set(authorizersToLink)
        recordToUpdate['Authorizers'] = list(dedupe)
    if recordToUpdate:
        recordsToUpdate.append({'id': account.get('id'), 'fields': recordToUpdate})
    #add to the update record
pprint(len(recordsToUpdate))
accountsTable.batch_update(recordsToUpdate)
#update all jc accounts


