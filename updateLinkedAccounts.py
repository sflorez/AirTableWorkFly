from pyairtable import Table
from pyairtable.formulas import match
import airTableApi
import csv
from pprint import pprint

baseId = 'appPo5a8lOdeAEI6D'
accountsTableName = 'All Accounts'
flyEmployeesTableName = 'flyExclusive Employees'

accountsTable = airTableApi.getTable(baseId=baseId, tableName=accountsTableName)
flyEmployeeTable = airTableApi.getTable(baseId=baseId, tableName=flyEmployeesTableName)

# update Accounts table with sales_rep_id from postgres
# salesRepData = []
# with open('allSalesRepIds.csv') as csvfile:
#     salesRep = csv.DictReader(csvfile, delimiter=',')
#     for row in salesRep:
#         salesRepData.append(row)

# pprint(salesRepData)
# for records in accountsTable.iterate(page_size=100):
#     recordsToUpdate = []
#     for record in records:
#         # pprint(record['fields']['customer_id'])
#         matchingSalesRepId = filter(lambda x: x['id'] == record['fields']['customer_id'], salesRepData)
#         listOfReps = list(matchingSalesRepId)
#         if len(listOfReps) > 0:
#             pprint(listOfReps[0])
#             recordsToUpdate.append({'id' : record['id'], 'fields': {'sales_rep_ps' : listOfReps[0]['sales_rep_id']}})
#     accountsTable.batch_update(recordsToUpdate)


# link to flyEmployees
for records in accountsTable.iterate(page_size=100):
    recordsToUpdate = []
    for record in records:
        # pprint(record['fields'].get('sales_rep_ps'))
        recordSalesRepPs = record['fields'].get('sales_rep_ps')
        if recordSalesRepPs is not None and recordSalesRepPs != 'NULL':
            pprint(recordSalesRepPs)
            formula = match({'employee_id copy' : recordSalesRepPs})
            flyEmployeeRecord = flyEmployeeTable.first(formula=formula)
            if flyEmployeeRecord:
                pprint(flyEmployeeRecord)
                idToLink = flyEmployeeRecord['id']
                recordsToUpdate.append({'id' : record['id'], 'fields':{'sales_rep_id': [idToLink]}})
    accountsTable.batch_update(recordsToUpdate)


