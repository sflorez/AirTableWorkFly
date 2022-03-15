from utils import getCSVData
from pprint import pprint

data = getCSVData(filepath='Transactions-personal view.csv', withCodec="utf-8-sig", delimiter=',')

pprint(data)

transactionIds = [el.get('Transaction ID') for el in data]

pprint(transactionIds)

smileDirectData = getCSVData(filepath='smileDirectClubExpenses.csv', withCodec=None, delimiter=',')

smileFormatted = list(map(lambda x: {'id' : x.get('id'), 'amount' : x.get('amount')}, smileDirectData))
pprint(smileFormatted)
#pprint(smileDFormatted)

matches = []
for row in smileFormatted:
    match = [el for el in data if el.get('Transaction ID') == row.get('id') and el.get('Amount') == row.get('amount')]
    if match:
        pprint("match!")
        matches.append(match)
    else:
        pprint('no match:')
        pprint(row)

pprint(len(matches))

##{'id' : x.get('id'), 'amount' : x.get('amount')}
