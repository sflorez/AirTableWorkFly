import requests
from pprint import pprint
import json
import csv
import airTableApi
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import dotenv
import utils
import os

#TO - DO
#Create a sync that takes the lines items from the workorder and put creates discrepencies from them instead of specific endpoints
#Create the url mapping formatting the data
#Remove sub items (there are too many)

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)
flightdocs_uri = 'https://mxapi.atp.com'
flightdocs_token = 'eyJhbGciOiJSUzI1NiIsImtpZCI6IjEzODgzMzlERDUwRjczNTVBNEE0NTVDM0MyQzM4MzM5RkI2NkVEMEQiLCJ0eXAiOiJhdCtqd3QiLCJ4NXQiOiJFNGd6bmRVUGMxV2twRlhEd3NPRE9mdG03UTAifQ.eyJuYmYiOjE2Nzc1MzMxMjAsImV4cCI6MTcxMDI3ODcyMCwiaXNzIjoiaHR0cHM6Ly9hdXRoLmZsaWdodGRvY3MuY29tIiwiYXVkIjoiZW50YXBpIiwiY2xpZW50X2lkIjoiMDczODQzMGQtYTlkNi00MDdiLWJmOWQtOTEyNGFkMDc0YTZmIiwic3ViIjoiMDAwMjAwMDAtYWMxMS0wMjQyLTAwNTUtMDhkYjA5MWE1ZTM0IiwiYXV0aF90aW1lIjoxNjc3NTMzMDkyLCJpZHAiOiJsb2NhbCIsImVtYWlsIjoia3NlbmtldmljaEBhdHAuY29tIiwiaHR0cDovL2ZsaWdodGRvY3MuY29tL2lkZW50aXR5L2NsYWltcy91c2VyaWQiOjU2OTA0LCJ1bmlxdWVfbmFtZSI6ImludGVncmF0aW9uX2ZseWV4Y2x1c2l2ZSIsIm5hbWUiOiJJbnRlZ3JhdGlvbiBVc2VyIiwiZ2l2ZW5fbmFtZSI6IkludGVncmF0aW9uIiwiZmFtaWx5X25hbWUiOiJVc2VyIiwiaHR0cHM6Ly9mbGlnaHRkb2NzLmNvbS9pZGVudGl0eS9jbGFpbXMvY2xpZW50aWQiOiIwNzM4NDMwZC1hOWQ2LTQwN2ItYmY5ZC05MTI0YWQwNzRhNmYiLCJodHRwOi8vZmxpZ2h0ZG9jcy5jb20vaWRlbnRpdHkvY2xhaW1zL2N1c3RvbWVyaWQiOjEzMDA4MjAsImh0dHA6Ly9mbGlnaHRkb2NzLmNvbS9pZGVudGl0eS9jbGFpbXMvY3VzdG9tZXJsb29rdXBpZCI6IjAwMDMwMDAwLWFjMTEtMDI0Mi0zM2I2LTA4ZGE4NzYyMDkzYiIsImh0dHA6Ly9mbGlnaHRkb2NzLmNvbS9pZGVudGl0eS9jbGFpbXMvaXNzeXRlbWFjY291bnQiOmZhbHNlLCJodHRwOi8vZmxpZ2h0ZG9jcy5jb20vaWRlbnRpdHkvY2xhaW1zL2lzZW1wbG95ZWUiOmZhbHNlLCJzY29wZSI6WyJvcGVuaWQiLCJwcm9maWxlIiwiZW1haWwiLCJBaXJjcmFmdFRpbWVzIiwiRHVlTGlzdCIsIkFpcmNyYWZ0QXZhaWxhYmlsaXR5IiwiTm9uUm91dGluZUl0ZW1zIiwiUHVyY2hhc2VPcmRlckxpbmVJdGVtcyIsIldvcmtPcmRlcnMiLCJMb2dib29rcyIsIk14SW50ZWdyYXRpb24iLCJvZmZsaW5lX2FjY2VzcyJdLCJhbXIiOlsicHdkIl19.lMa4CKyxPgirVKrEDkPFi8MFHHQD_zF3EUvzX6gz0fCVMfc_HmKyNxww0Wiz8KQFfXCCK7CWJTwdQ5DTkgL-uCj3JwFRPyfOQ4GHSCPrWyp1SlQvRh7fmliZklxuUPE3W1XuBc08tkOUykJTjBwKxhEClzKsNYGytY5FHEgLeO6ZWZoB9sXAgKeINFEk5yQTBmXszDpDn12XkgCdk2lgIprKzWjcn2USBl5i_QQkhB9NEjZrZ2U43P05yUtntbbGvGGjelNC6VEJazrOIArSpumEc3rgkGaEHkuGqPWS-kdAlAPn44LUJCBJ41JcNVhuKboT6FW70wB603ObQ08w0GIzg2ROetDJm3ZQZ1JpPqHK5eIpm6LBTioDEUBTEWFdVQcdwRv7pVWTCQ8AU_NLJ5QqPBG7st4xjJFYBSrsKrWzeD3FfV5s-KMTCpiQ08hpoebtwCQS_fkCmYDt1Jbhjrka6uDvP8v_W36Ee7AfLuGFz-3NNyO84N9yLofnyYq2Uem_jVcCSFOi0zN42t3bE3s5G5d9jwGyIpB1POFBpCMjNBwTWq42FqL87TI7nY9wKmaln-er64Lti7kNod09arRgx4poH9e5DZcC7SD2T-I3DxxXJ514V0X0nqR9hWpcPPdVFtAfIDvd8YExGXwch3AO4m-VmOlFvCNEULpIzPE'

airtable_workorder_sync = 'https://api.airtable.com/v0/appfhUhbEsTFZpDJv/tblDodWeahMZw19WE/sync/xPO1iscj'
airtable_maintenance_sync = 'https://api.airtable.com/v0/appfhUhbEsTFZpDJv/tblMcicOoZ8d5N5mL/sync/n7YNGSno'
airtable_aog_sync = 'https://api.airtable.com/v0/appfhUhbEsTFZpDJv/tbl3wMK61XS6180J4/sync/GtYhXTDw'
airtable_discrepancies_sync = 'https://api.airtable.com/v0/appfhUhbEsTFZpDJv/tblEtpTnIVxV9gdbA/sync/oFypwW1s'
personal_sync_token = 'patBEggw1ccVHJNTg.4e86c4f5880a0b75c216fcea39cd423f24d9beac4cec3dfa7a4dc6172e7009e5'
flight_docs_base = 'appfhUhbEsTFZpDJv'
work_orders_table_id = 'tblXHmArOS4SDLOYV'
work_orders_table = airTableApi.getTable(flight_docs_base, work_orders_table_id)

airtable_sync_headers = {
    'Authorization' : f'Bearer {personal_sync_token}',
    'Content=Type' : 'text/csv; charset=utf-8'
}

headers={
    'content-type': 'application/json',
    'Accept': 'application/json',
    'api-version' : '2.0',
    'Authorization': f'Bearer {flightdocs_token}'}

#network functions
def flightdocs_get(query,params=None):
    # pprint(query)
    res = requests.get(f'{flightdocs_uri}/{query}?{params}',headers=headers)
    # pprint(res.json())
    return res.json()

def flightdocs_post(query, payload):
    # pprint(query)
    res = requests.post(f'{flightdocs_uri}/{query}',headers=headers, data=json.dumps(payload))
    pprint(res)
    #pprint(res.json())
    return res.json()

#flight docs functions
def get_flight_docs_aircraft():
    aircraft = flightdocs_get('Aircraft/getmyaircraft')
    return aircraft['Data']

def get_flight_docs_maintenance_due_list(aircraft_registration):
    maintanence_due_list = flightdocs_get(f'MaintenanceItem/GetDueList/{aircraft_registration}')
    # pprint(maintanence_due_list['Data'])
    return maintanence_due_list['Data']

def get_flight_docs_non_routine_items(aircraft_registration):
    non_routine = flightdocs_get(f'NonRoutineMaintenanceItem/search', f'RegistrationNumber={aircraft_registration}')
    return non_routine['Data']

def get_flight_docs_work_orders(aircraft_ids):
    payload = {"AircraftIds": aircraft_ids, 
               "PageSize" : 100000,
               "IncludeLineItems": 'true',
               }
    res = flightdocs_post('WorkOrder/getworkorders', payload)
    return res['Data']

def get_flight_docs_work_orders_created_since(aircraft_ids, last_updated):
    payload = {"AircraftIds": aircraft_ids, 
            "PageSize" : 100000,
            "IncludeLineItems": 'true',
            "MinimumCreated" : last_updated
            }
    res = flightdocs_post('WorkOrder/getworkorders', payload)
    return res['Data']

def get_flight_docs_work_orders_updated_since(aircraft_ids, last_updated):
    payload = {"AircraftIds": aircraft_ids, 
            "PageSize" : 100000,
            "IncludeLineItems": 'true',
            "ChangedSince" : last_updated
            }
    res = flightdocs_post('WorkOrder/getworkorders', payload)
    return res['Data']

# airtable sync functions
def publish_to_sync(sync_url, file_name):
    with open(f'{file_name}.csv', 'r', encoding='utf8') as f:
        s = f.read()
        res = requests.post(sync_url, headers=airtable_sync_headers, data=s.encode('utf-8'))
        pprint(res)

def save_temp_data(data, file_name):
    with open(f'{file_name}.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, data[0].keys() ,)
        writer.writeheader()
        writer.writerows(data)

def format_workorder(wo):
    keys_to_remove = ['ContactEmail', 
                      'CompletedHoursPercent', 
                      'CompletedHours', 
                      'ClosedLineItemPercent', 
                      'ActualTotalCost', 
                      'ActualLaborCostSubTotal', 
                      'ActualInventoryCostSubTotal', 
                      'AircraftId', 
                      'TimeZone', 
                      'WorkOrderStatusId', 
                      'CreatedBy', 
                      'LastUpdatedBy', 
                      'AirportId', 
                      'LineItems', 
                      'AccountCode', 
                      'ActualAdditionalCostSubTotal',
                      'ContactName',
                      'ContactPhone',
                      'EndDate',
                      'EstimatedAdditionalCostSubTotal',
                      'EstimatedLaborCostSubTotal',
                      'EstimatedTotalCost',
                      'InReviewLineItemCount',
                      'IsActive',
                      'RemainingHours',
                      'RequireSignatureLineItemCount',
                      'StartDate',
                      'TermsAndConditions',
                      'EstimatedInventoryCostSubTotal',
                      'EstimatedHours'
                      ]
    bad_tail_numbers = ['141187', 'CAE330344', 'CAE330537', 'Eng DE0093' , 'Eng DE0094' , 'Eng DE0100', 'Eng PCE-DE00', 'PCE-DB0333', 'PCE-DE0048', 'PCE-DE0074']
    workorder_url = f'https://app2.flightdocs.com/#/work-orders/work-order/detail/{wo["Id"]}'
    for key in keys_to_remove:
        wo.pop(key)
    
    for tail in bad_tail_numbers:
        if(wo.get('Aircraft_RegistrationNumber', None) == tail):
            wo.pop('Aircraft_RegistrationNumber')
    
    wo['FlightDocsURL'] = workorder_url
    return wo

def format_line_items(li):
    pprint(li)
    # li['WorkOrderNumber'] = 
    return li

def add_created_work_orders(aircraft_ids):
    wos = get_flight_docs_work_orders_created_since(aircraft_ids, os.getenv('LAST_UPDATED'))
    pprint(wos)
    current_data = utils.readJsonData('work_orders')
    work_orders_to_create= []
    for wo in wos:
        wo_formatted = format_workorder(wo)
        if not wo['LookupId'] in current_data:
            work_orders_to_create.append(wo_formatted)
    created_work_orders = work_orders_table.batch_create(work_orders_to_create, typecast=True)
    pprint(created_work_orders)
    formatted_data = {el['fields']['LookupId'] : el['id'] for el in created_work_orders}
    current_data.update(formatted_data)
    utils.writeJsonData(current_data, 'work_orders')

def update_work_orders(aircraft_ids):
    wos = get_flight_docs_work_orders_updated_since(aircraft_ids, os.getenv('LAST_UPDATED'))
    pprint(len(wos))
    work_orders_to_update=[]
    current_data = utils.readJsonData('work_orders')
    for wo in wos:
        wo_formatted = format_workorder(wo)
        update_data = {"id" : current_data[wo['LookupId']], "fields" : wo_formatted}
        work_orders_to_update.append(update_data)
    work_orders_table.batch_update(work_orders_to_update, typecast=True)

#get all aircraft from flightdocs
aircraft = get_flight_docs_aircraft()
aircraft_ids = list(map(lambda x: x['Id'], aircraft))
aircraft_regs = list(map(lambda x: x['RegistrationNumber'], aircraft))

add_created_work_orders(aircraft_ids)
update_work_orders(aircraft_ids)
dotenv.set_key(dotenv_file, 'LAST_UPDATED', datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))
# pprint(aircraft_ids)

# get and publish maintenance discrepencies
# maintanence_due_list = get_flight_docs_maintenance_due_list('N828JS')
# save_temp_data(maintanence_due_list, 'mx_data')
# publish_to_sync(airtable_maintenance_sync, 'mx_data')

#get and publish non routine discrepencies
# non_routine = get_flight_docs_non_routine_items('N821JS')
# pprint(non_routine)
# save_temp_data(non_routine, 'non_routine_data')
# publish_to_sync(airtable_aog_sync, 'non_routine_data')

# publish all non-routine items
# with ThreadPoolExecutor(max_workers=10) as pool:
#     full_non_routine = list(pool.map(get_flight_docs_non_routine_items, aircraft_regs))
# flat_list = [item for sublist in full_non_routine for item in sublist]
# pprint(len(flat_list))
# save_temp_data(flat_list, 'non_routine_data')
# publish_to_sync(airtable_aog_sync, 'non_routine_data')

#get and publish all workorders
# wos = get_flight_docs_work_orders(aircraft_ids)
# wos = get_flight_docs_work_orders_since(aircraft_ids, os.getenv('LAST_UPDATED'))
# pprint(wos)
# work_orders_to_create= []
# for wo in wos:
#     wo_formatted = format_workorder(wo)
#     work_orders_to_create.append(wo_formatted)
# created_work_orders = work_orders_table.batch_create(work_orders_to_create, typecast=True)
# pprint(created_work_orders)
# # airtable_work_orders = work_orders_table.all(fields=['LookupId'])
# formatted_data = {el['fields']['LookupId'] : el['id'] for el in created_work_orders}
# # pprint(formatted_data)
# current_data = utils.readJsonData('work_orders')
# current_data.update(formatted_data)
# utils.writeJsonData(current_data, 'work_orders')
# utils.writeJsonData(formatted_data, 'work_orders')
# 


# full_items = []
# for wo in wos:
#     items_to_append = wo['LineItems']
#     for item in items_to_append:
#         item['WorkOrderNumber'] = wo['Number']
#     full_items.append(items_to_append)
# flat_list = [item for sublist in full_items for item in sublist]
# # filter out the subitems
# sub_removed = (list(filter(lambda x: x['ChildWoliNumber'] == None, flat_list)))
# wos_formatted = list(map(format_workorder, wos))
# save_temp_data(wos_formatted, 'work_order_data')
# publish_to_sync(airtable_workorder_sync, 'work_order_data')
# save_temp_data(sub_removed, 'discrepancy_data')
# publish_to_sync(airtable_discrepancies_sync, 'discrepancy_data')


# pprint(len(list(filter(lambda x: x['MaintenanceItemTypeId'] == 2, flat_list))))
# pprint(len(list(filter(lambda x: x['MaintenanceItemTypeId'] == 5, flat_list))))
# left_over = list(filter(lambda x: x['MaintenanceItemTypeId'] != 5 and x['MaintenanceItemTypeId'] != 2, flat_list))
# pprint(set(map(lambda x: str({x['MaintenanceItemTypeId'] : x['MaintenanceItemType_Name']}), flat_list)))
# pprint(left_over[1:20])
# wos_formatted = [dict([("Number", i.pop('Number'))]) | i for i in wos]
# save_temp_data(wos_formatted, 'work_order_data')
# publish_to_sync(airtable_workorder_sync, 'work_order_data')


# # pprint(len(aircraft_regs))
# # data_file = open('data_file.csv', 'w')
# # csv_writer = csv.writer(data_file)
# # pprint(len(workOrdertest['Data']))
# workorder_test_data = workOrdertest['Data']

# for tail in test:
#     AIRTABLE_DISCREPANCY_URL = 'https://api.airtable.com/v0/appy6h4lVtoLr35bn/tblXqnAh2Tre8jdsy' # env variable
#     at_discrepancies = requests.get(f"{AIRTABLE_DISCREPANCY_URL}?filterByFormula=%7BAircraft_bi%7D%3D'{tail}'",headers={'Accept': 'application/json', 'Content-Type':'application/json', 'Authorization': 'Bearer keyheAMFKImzbPnvZ'})
#     disc_data = at_discrepancies.json()['records']
#     at_records = []
#     for d in disc_data:
#         at_id = d['id']
#         at_c_num = d['fields']['Control Number']
#         at_mel_num = d['fields']['MEL Number']
#         at_desc = d['fields']['Description']
#         if not at_c_num or not at_mel_num or not at_desc:
#             continue
#         at_records.append({'at_record_id':at_id, 'discrepancy_id': at_c_num+at_mel_num+at_desc})

    # nr_mx_items = flightdocs_get(f'NonRoutineMaintenanceItem/search',f'RegistrationNumber={tail}')
    # pprint(nr_mx_items)
#     linkables = []
#     for item in nr_mx_items['Data']:
#         control_num = item['ControlNumber']
#         mel_num = item['ManufacturingMaintenanceCode']
#         description = item['Description']
#         if not control_num or not mel_num or not description:
#             continue
#         # pprint(item)
#         equipment_id = item['EquipmentId']
#         item_num = item['ItemNumber']
#         link = f'https://app2.flightdocs.com/#/maintenance/non-routine/detail/{equipment_id}/{item_num}'
#         linkables.append({'id': control_num+mel_num+description, 'link': link})
# pprint(linkables)


    

    