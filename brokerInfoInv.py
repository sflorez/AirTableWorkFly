from utils import getCSVData
from pprint import pprint
import codecs

brokerInfo = getCSVData('AccountBrokerInformationCSV.csv', withCodec="utf-8-sig", delimiter=',')
accountNamePG = getCSVData('accountNamesPostgres.csv', withCodec=None, delimiter=',')
#pprint(brokerInfo[1:5])

brokerNames = [el.get('Account') for el in brokerInfo]
accountNames = [el.get('account_name') for el in accountNamePG]
#pprint(brokerNames[1:5])


uniqueBrokerNames = set(brokerNames)
uniqueAccountNames = set(accountNames)
# pprint(uniqueBrokerNames)
pprint(len(uniqueBrokerNames))
pprint(len(uniqueAccountNames))
pprint(len(set.intersection(uniqueBrokerNames, uniqueAccountNames)))
differenceNames = uniqueBrokerNames.difference(uniqueAccountNames)
pprint(len(differenceNames))
##pprint(list(differenceNames)[0:100])