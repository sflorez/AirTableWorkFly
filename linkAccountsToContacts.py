import airTableApi
from utils import Bases, TestBases
from utils import getCSVData
from pprint import pprint

base = Bases.get('CRM')
accountsTable = airTableApi.getTable(baseId=base, tableName='All Accounts')
contactsTable = airTableApi.getTable(baseId=base, tableName='Contacts')

roles = {1 : 'primary',
         2 : 'authorizer',
         3 : 'passenger'}

linkData = getCSVData(filepath='CSVs/otherAccounts.csv', withCodec='utf-8-sig', delimiter=',')

# pprint(linkData)

# jetClubAccounts = accountsTable.all(view='Accounts with no contacts')
# allContacts = contactsTable.all(fields=['primary_id','contact_primary_id', 'avianis_primary_id'], view='Avianis Contacts')
recordsToUpdate = []
badRecords = []

def getContactRecord(contactId):
    # pprint(contactId)
    contactRecord = [el for el in allContacts if el['fields'].get('avianis_primary_id') == contactId]
    # pprint(contactId)
    if contactRecord:
        return contactRecord[0]
    else:
        return None

for account in jetClubAccounts:
    primaryToLink = []
    contactsToLink = []
    # authorizersToLink = []
    recordToUpdate = {}
    contactLinkData = [el for el in linkData if el.get('customer_id') == account['fields'].get('av_customer_id')]
    # pprint(contactLinkData)
    primary = [el.get('contact_id') for el in contactLinkData if el.get('is_primary_contact') == 'True']
    # pprint(primary)
    # contacts = [el.get('contact_id') for el in contactLinkData if roles.get(int(el.get('role_id'))) == 'passenger']
    # authorizers = [el.get('contact_id') for el in contactLinkData if roles.get(int(el.get('role_id'))) == 'authorizer']
    if primary:
        #lookup primary contact info
        print('primary stuff')
        possibleLink = getContactRecord(primary[0])
        pprint(possibleLink)
        if possibleLink:
            primaryToLink.append(possibleLink.get('id'))
            recordToUpdate['Primary'] = primaryToLink
            # contacts.append(primary[0])
    # if authorizers:
    #     #lookup auth bois
    #     print('auth stuff')
    #     for authorizer in authorizers:
    #         possibleLink = getContactRecord(authorizer)
    #         if possibleLink:
    #             contacts.append(authorizer)
    #             authorizersToLink.append(possibleLink.get('id'))
    #     dedupe = set(authorizersToLink)
    #     recordToUpdate['Authorizers'] = list(dedupe)
    # if contacts:
        #lookup contacts infos
    # print('contact stuff')
    for contact in contactLinkData:
        possibleLink = getContactRecord(contact.get('contact_id'))
        if possibleLink:
            contactsToLink.append(possibleLink.get('id'))
    dedupe = set(contactsToLink)
    recordToUpdate['contacts'] = list(dedupe)
    if recordToUpdate:
        recordsToUpdate.append({'id': account.get('id'), 'fields': recordToUpdate})
    else:
        badRecords.append(account)
    #add to the update record
pprint(recordsToUpdate)
pprint(len(recordsToUpdate))
pprint(badRecords)
# accountsTable.batch_update(recordsToUpdate)
#update all jc accounts


