import codecs
import csv

Bases = {'Sales' : 'apphXonxcXMIfE8qm',
         'Trips' : 'appxCRpKCTUJSZ9At', 
         'CRM' : 'apppywY8da44n71Ee',
         'Finance' : 'appl5oSDrBVZ9geuj',
         'Static' : 'appamPDLTB6vyeNDB',
         }

TestBases = {'CRM': 'appqVB2CntdQaBWmE'}

def getCSVData(filepath, withCodec, delimiter):
    data = []
    if withCodec:
        with codecs.open(filepath, encoding=withCodec, errors='replace') as csvfile:
            rows = csv.DictReader(csvfile, delimiter=delimiter)
            for row in rows:
                data.append(row)
    else:
        with open(filepath) as csvfile:
            rows = csv.DictReader(csvfile, delimiter=delimiter)
            for row in rows:
                data.append(row)
    return data
