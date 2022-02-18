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

def linkTo(linkFromTable, linkToTable, chunkSize, referencefield, comparisonfield, fieldToLink):
    for records in linkFromTable.iterate(page_size=chunkSize):
        recordsToUpdate = []
        for record in records:
            referenceId = record['fields'].get(referencefield)
            if referenceId:
                formula = match({ comparisonfield : referenceId})
                referenceRecord = linkToTable.first(formula=formula)
                if referenceRecord:
                    idToLink = referenceRecord['id']
                    recordsToUpdate.append({'id': record['id'], 'fields' : {fieldToLink : [idToLink]}})
        pprint(recordsToUpdate)
        linkFromTable.batch_update(recordsToUpdate)


