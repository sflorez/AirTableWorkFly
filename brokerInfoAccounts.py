from utils import getCSVData
from pprint import pprint
import codecs

brokerInfoAgg = getCSVData('charterBrokers_agg.csv', withCodec="utf-8-sig", delimiter=',')
brokerInfo = getCSVData('AccountBrokerInformationCSV.csv', withCodec='utf-8-sig', delimiter=',')
accountNamePG = getCSVData('accountNamesPostgres.csv', withCodec=None, delimiter=',')
aTaccounts = getCSVData('All Accounts-TestView.csv', withCodec='utf-8-sig', delimiter=',')
#pprint(brokerInfo[1:5])

brokerNamesAgg = [el.get('Column1') for el in brokerInfoAgg]
accountNames = [el.get('account_name').lower() for el in accountNamePG]
atNames = [el.get('Name').lower() for el in aTaccounts]
brokersNames = [el.get('Account').lower() for el in brokerInfo]
#pprint(brokerNames[1:5])

uniqueBrokerNamesAgg = set(brokerNamesAgg)
uniqueAccountNames = set(accountNames)
uniqueAtNames = set(atNames)
uniqueBrokerNames = set(brokersNames)
# pprint(uniqueBrokerNames)
pprint(len(uniqueBrokerNames))
pprint(len(uniqueAtNames))
# pprint(len(uniqueBrokerNamesAgg))
# pprint(len(uniqueAccountNames))
# pprint(len(set.intersection(uniqueBrokerNamesAgg, uniqueAccountNames)))
differenceNames = uniqueBrokerNamesAgg.difference(uniqueAccountNames)
# pprint(len(differenceNames))
combinedAccounts = uniqueBrokerNames.union(uniqueAccountNames)
pprint("total accounts: ")
pprint(len(combinedAccounts))
pprint(len(set.intersection(combinedAccounts, uniqueAtNames)))
#pprint(uniqueAtNames.difference(combinedAccounts))
##pprint(list(differenceNames)[0:100])