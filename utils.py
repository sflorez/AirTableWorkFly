import codecs
import csv
from dataclasses import field

Bases = {'Sales' : 'apphXonxcXMIfE8qm',
         'Trips' : 'appxCRpKCTUJSZ9At', 
         'CRM' : 'apppywY8da44n71Ee',
         'Finance' : 'appl5oSDrBVZ9geuj',
         'Static' : 'appamPDLTB6vyeNDB',
         }

TestBases = {'CRM': 'appazV6FfzJihCso9'}

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

def writeCSVFile(header, data, encoding, newline):
    with open('recent_av_contacts_to_create.csv', 'w', encoding=encoding, newline=newline) as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(data)

    
