import os
from dotenv import load_dotenv
from pyairtable import Table
from pprint import pprint

load_dotenv()

api_key = os.getenv('API_KEY')

def getTable(tableId, tableName):
    table = Table(api_key, tableId, tableName)
    return table
