import os
from dotenv import load_dotenv
from pyairtable import Table
from pyairtable.formulas import match
from pprint import pprint

load_dotenv()

api_key = os.getenv('API_KEY')

def getTable(baseId, tableName):
    table = Table(api_key, baseId, tableName)
    return table

# this should be optimized to use a memoized list 
def linkTo(linkFromTable, linkToTable, chunkSize, referencefield, comparisonfield, fieldToLink):
    # get the records from the air table to link to 
    recordsToLinkTo = linkToTable.all()

    for records in linkFromTable.iterate(page_size=chunkSize):
        recordsToUpdate = []
        for record in records:
            referenceId = record['fields'].get(referencefield)
            if referenceId:
                # instead of matching using a formula that we pass to airtable use list comprehension to find the record we want
                #formula = match({ comparisonfield : referenceId})
                #referenceRecord = linkToTable.first(formula=formula)
                referenceRecords = [el for el in recordsToLinkTo if el['fields'].get(comparisonfield) == referenceId]
                if len(referenceRecords) > 0:
                     idToLink = referenceRecords[0]['id']
                     recordsToUpdate.append({'id': record['id'], 'fields' : {fieldToLink : [idToLink]}})
        pprint(recordsToUpdate)
        linkFromTable.batch_update(recordsToUpdate)
        

def linkToMultiple():
    return None


