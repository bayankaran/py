import requests
import os
import re
import sys
from decimal import Decimal
import datetime
from numbers import Number
import boto3
import paramiko
import logging
import json

logger = logging.getLogger('cc->nessie')
logger.propagate = False
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(filename)s - %(name)s - <%(levelname)s>: %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

USE_ACCOUNTNAME = False

account_id_and_monthly_bill = {}

# Temporary dictionary to store WBS codes
# TODO To be taken to Dyanamo DB

key_accnt_value_wbs = {
    "atos-csi-aws" :  'NL.544893.010',
	"atos-testcustomer-aws" : 'NL.544894.010'
}

# dictionary to keep multi_account name and MonthlyToDateCost
mav_cost_dict = {}

# List of multi account views in test environment
# TODO to be fetched from DynamoDB
use_account_mav = ['atos-csi-aws', 'atos-testcustomer-aws']

base_url = 'https://eu.cloudcheckr.com/api/'
resgroup_pattern = re.compile(r"\/resourceGroups\/([^\/]+)")

# global variables - account_id, monthly_cost for AWS & Azure
aws_acct_id_mnthly_cost = dict()
azure_acct_id_mnthly_cost = dict()
accountId_WBS_Code = dict()
account_id_list_final = []

# Function to flatten a list...
def flat_the_list(items):
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            for sub_x in flat_the_list(x):
                yield sub_x
        else:
            yield x

# Function to pretty print a JSON file...
def pp_json(json_thing, sort=True, indents=4):
    if type(json_thing) is str:
        print(json.dumps(json.loads(json_thing), sort_keys=sort, indent=indents))
    else:
        print(json.dumps(json_thing, sort_keys=sort, indent=indents))
    return None
    
# Function to pull out values for specific keys
# returns a list object
def extract_key_value(key, var):
    if hasattr(var,'items'):
        for k, v in var.items():
            if k == key:
                yield v
            if isinstance(v, dict):
                for result in extract_key_value(key, v):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in extract_key_value(key, d):
                        yield result

def main(apiKey, master_apiKey, *args, **kwargs):
    
    cur_time = datetime.datetime.now().strftime('%m-%d-%Y')
    logger.info ('Date: ' + str(cur_time))

    try:
 
        # accountId_WBS_Code, aws_acct_id_mnthly_cost, azure_acct_id_mnthly_cost = get_accounts_v4(apiKey, apiKeyV4, customerCode, useAccount)
        
        mav_cost_dict = get_mav_monthly(use_account_mav, apiKey, master_apiKey)
        
        logger.info('')
        for key, value in mav_cost_dict.items() :
            logger.info('multi account name: ' +str(key)+ ', monthly_cost: ' + str(value))

    except:
        logger.exception("API failure...")
        raise
    
    try:
        
        acctId_monthly_cst_aws_azure = []
        acctId_monthly_cst_aws_azure_wbs = []
        
        for x,y in aws_acct_id_mnthly_cost.items():
            # capture currency, strip, append
            acctId_monthly_cst_aws_azure.append([x,y])
        
        for x, y in azure_acct_id_mnthly_cost.items():
            acctId_monthly_cst_aws_azure.append([x,y])
                
        for x in acctId_monthly_cst_aws_azure:
            id_for_match_1 = x[0]
            
            for id, y in key_accnt_value_wbs.items():
            
                if id_for_match_1 == str(id):
                    ## strip only the WBS code
                    wbs_code = str(y)
                    acctId_monthly_cst_aws_azure_wbs.append([x,wbs_code])                    
        
    except:
        logger.exception("List(s) creation for CSV failure...")
        raise
        
    try: 
    
        file_template = 'MPC_{date}_{time}'
        
        date_for_file = datetime.datetime.now()
        
        name_params = {
            'date': date_for_file.strftime('%Y%m%d'),
            'time': date_for_file.strftime('%H%M%S'),
        }

        filenames = [
            '/tmp/MONTHLY_COST_%s.txt' % file_template.format(**name_params)
        ]

    except:
        logger.exception('File (txt) creation failure...')
    
    try:
    
        with open(filenames[0], 'w') as csv_file:
            
            logger.info("Writing data file '%s'", filenames[0])

            prev_month, prev_month_yyyy, cur_month, cur_day = return_month_day_range()
            
            logger.info('prev_month: ' + prev_month + ', current_month: ' + cur_month + ', cur_day: ' + str(cur_day))
            
            # TODO 
            # sequence number, to be fetched from DynamoDB
            seq_no = '000001'
            
            rec_identifier = 'CC-PC-invc'
            
            # Header record
            csv_file.write('H' + str(rec_identifier) + str(seq_no) + str(cur_day) + str(prev_month) + '\n')
            
            # flatten the list to strings
            # logger.info('')
            logger.info('account id, cost, wbs, currency')
            rec_count = 0
            len_rec_count = 0
            
            for key, value in mav_cost_dict.items():
                
                accnt_id = key
                cost = value

                # Strip currency from cost and remove ',' from cost
                # TODO the dwalar value should be fetched from the DynamoDB
                dwalar = True
                
                if cost >= 0.00:
                    type = '+'
                else:
                    type = '-'
                
                final_cost = str("{0:.2f}".format(cost))
                final_cost = re.sub('[.]','',str(final_cost)).zfill(10)
                
                for key, wbs in key_accnt_value_wbs.items():
                    if key == accnt_id:
                        csv_file.write('W' +(str(wbs))+'           '+(str(final_cost))+(str(type)))
                        if dwalar:
                            csv_file.write('USD')
                        else: 
                            csv_file.write('EUR')
                        csv_file.write('X')
                        csv_file.write('\n')
                        rec_count = rec_count + 1
                        
            # for trailer record
            len_rec_count = len(str(rec_count))
            logger.info ('len_rec_count: ' + str(len_rec_count))
            csv_file.write('T' + str(rec_identifier) + str(seq_no) + str(rec_count).zfill(7 - len_rec_count))
    except:
        logger.exception('File writing failure...')
        
    try:
        
        logger.info('SFTP call')
        # send_files(filenames, controls)
    
    except:
    
        logger.exception("Unable to send the files to the SFTP destination")
        raise
    
    logger.info("All operations for master api key ending in '%s' concluded successfully", master_apiKey[-6:])   

def send_files(filenames, controls):
    logger.info("Beginning file transfer")
    sftp_host = os.environ['SFTP_HOST']
    sftp_port = int(os.environ['SFTP_PORT'])
    sftp_user = os.environ['SFTP_USER']
    sftp_priv_key = os.environ['SFTP_PRIV_KEY']
    sftp_piv_key_file = "/tmp/prv.pem"
    sftp_destination_path = os.environ['SFTP_DESTINATION_PATH']

    priv_key_inside = sftp_priv_key.replace("-----BEGIN RSA PRIVATE KEY-----", '').replace("-----END RSA PRIVATE KEY-----", '').replace(' ', '\n')

    with open(sftp_piv_key_file, "w") as sftp_prv_file: 
        sftp_prv_file.write("-----BEGIN RSA PRIVATE KEY-----\n")
        sftp_prv_file.write(priv_key_inside)
        sftp_prv_file.write("-----END RSA PRIVATE KEY-----")

    pk = paramiko.RSAKey.from_private_key_file(sftp_piv_key_file)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    client.connect(hostname=sftp_host, port=sftp_port, timeout=1, username=sftp_user, pkey=pk)
    sftp_client=client.open_sftp()
    for local_file in [*filenames, *controls]:
        logger.info("Transferring file '%s'", local_file)
        sftp_client.put(local_file, os.path.join(sftp_destination_path, os.path.basename(local_file)))
        logger.info("'%s' sent", local_file)
    sftp_client.close()
    
    logger.info("File transfer complete")


def get_mav_monthly(use_account_mav, apiKey, master_apiKey):
    
    access_key = master_apiKey
    
    prev_month, prev_month_yyyy, cur_month, cur_day = return_month_day_range()
        
    for multi_account in use_account_mav:
        
        logger.info ("Month used for filtering: " + str(prev_month_yyyy))
        result = requests.get(base_url + 'billing.json/get_monthly_bill', params={'access_key': access_key, 'cost_type':'List', 'use_account':multi_account, 'for_date':prev_month_yyyy}).json()
    
        logger.info('')
        logger.info(" Monthly Multi Account View Cost for " + multi_account +  " using master access key '%s' ", access_key[-8:])
        pp_json(result)
        
        # for each result, create a dictionary with 
        # multi_account view name and corresponding highest MonthlyToDateBill
        list_monthly_to_date_bill = list(extract_key_value('MonthlyToDateBill', result))
        
        # find the highest value of 'MonthlyToDateBill'
        max_value = 0.0
        for n in list_monthly_to_date_bill:
            n = re.sub('[$,]', '', n)
            if float(n) > max_value: max_value = float(n)
        
        mav_cost_dict.update({multi_account: max_value})
        
    return mav_cost_dict
    
    
# Function to get all accounts in an instance, using the provided API keys
def get_accounts_v4(apiKey, master_apiKey, customerCode, useAccount):
    use_account = useAccount
    access_key = master_apiKey
    url = '{base_url}/account.json/get_accounts_v4?access_key={access_key}'.format(base_url=base_url, access_key=access_key)

    result = requests.get(url).json()
    
    keyToSearch = 'cc_account_id'
    list_cc_accnt_id = list(extract_key_value(keyToSearch, result))
    
    keyToSearch = 'aws_account_id'
    list_aws_accnt_id = list(extract_key_value(keyToSearch, result))
    
    keyToSearch = 'azure_account_id'
    list_azure_accnt_id = list(extract_key_value(keyToSearch, result))
    
    logger.info('azure accout id(s): ' + str(list_azure_accnt_id))
    
    # For AWS, only using single account API keys
    monthly_cost_list = {}
    result_list = get_aws_accounts_for_monthly_bill(apiKey)
           
    for result in result_list:
        
        account_id_list = list(extract_key_value('Account', result))
        logger.info("list of AWS account ids found: " + str(account_id_list))
            
        for accnt_id in account_id_list: 
            
            # Strip account_id to the AWS id...
            accnt_id = accnt_id.split(None, 1)[0]
            
            logger.info('aws account id: ' + str(accnt_id))
            result_list = get_monthly_bill_per_account(apiKey, accnt_id)
            
            for result in result_list:
                logger.info('')
                monthly_cost_list = list(extract_key_value('MonthlyToDateCost', result))
            
                # create an account_id, monthly_cost dictionary for AWS
                for cost in monthly_cost_list:
                    if cost != '$0.00':
                        logger.info('')
                        logger.info("AWS Monthly cost for account id " + str(accnt_id) + ": " + str(cost))
                        aws_acct_id_mnthly_cost.update({accnt_id: cost}) 
        
    else:
        logger.info ('Not a valid account for given access key')
        
    
    # Loop for AWS
    for x in list_cc_accnt_id:
    
        if x != None:
            # for finding WBS codes
            accountId_WBS_Code = get_account(apiKey, apiKeyV4, x)
            
        
    # # # Loop for Azure - commented out for the first version of script
   
    # for x in list_cc_accnt_id:
        
        # if x != None:
            # result_azure = get_detailed_billing_with_grouping_v2_1(apiKey, apiKeyV4, x)
            # pp_json(result_azure)
            # keyToSearch_azure = 'GroupValue'
            # account_azure_list = list(extract_key_value(keyToSearch_azure, result_azure))
            # keyToSearch_azure = 'Cost'
            # cost_azure_list = list(extract_key_value(keyToSearch_azure, result_azure))
            # logger.info('account_azure_list: ' + str(account_azure_list))
            # logger.info('cost_azure_list: ' + str(cost_azure_list))
            # if len(account_azure_list) > 0:
                # azureAccount = account_azure_list.pop()
                # del account_azure_list[:]
                # if len(cost_azure_list) > 0: 
                    # azure_acct_id_mnthly_cost.update({azureAccount:cost_azure_list.pop()})
                    # del cost_azure_list[:]
        # else:
            # logger.info (str(x) + ' not a valid cc account id for Azure')
            
        # # Loop for GCP or other providers...
    
    return accountId_WBS_Code, aws_acct_id_mnthly_cost, azure_acct_id_mnthly_cost
   
# method for fetching Azure specific metrics
def get_detailed_billing_with_grouping_v2_1(apiKey, master_apiKey, cc_account_id):
    access_key = master_apiKey
    
    # for testing replacing with last month first day and last day. 
    start  =  '08-01-2018'
    end = '08-31-2018'
    
    result = requests.get(base_url + 'billing.json/get_detailed_billing_with_grouping_by_currency', params={'access_key': access_key, 'start': start, 'end': end, 'saved_filter_name': 'filter-monthly-account-costs', 'use_cc_account_id':cc_account_id}).json()
        
    logger.info('')
    logger.info ('Monthly cost for AZURE between ' + start + ' and ' + end + ' for cc account id:' + str(cc_account_id))
    return result
  
def return_month_day_range(when = None): 
    if not when:
        # Default today.
        when = datetime.datetime.today()
    # Find today.
    first = datetime.date(day=1, month=when.month, year=when.year)
    prev_month_end = first - datetime.timedelta(days=1)
    prev_month_start = datetime.date(day=1, month= prev_month_end.month, year= prev_month_end.year)
    prev_month = prev_month_end.strftime('%m%Y')
    cur_month = first.strftime('%m%y')
    prev_month_yyyy = prev_month_end.strftime('%Y-%m-%d') 
    cur_day = datetime.datetime.now().strftime('%d%m%y')
    return (prev_month, prev_month_yyyy, cur_month, cur_day)

    
if __name__ == '__main__':
    
    # list of account specific api keys
    #TODO Key should be externalized into Lambda environment variable, ciphered.
    apiKey = ['3N8HD98RV2L9V0SQF8U0S4B539136ABCV6F78A1R8632X17MA97F8Z749H3AVXKX', '6Z7R50H1JX1YXA9WVY234513DNFVGASC9JSN8QHX85758S419WLBK9H5BS4YPNUD']
    
    # super master api key
    master_apiKey = 'N1DJVNNL97I2R65TSAH27387BAB06074AMT3C547RKX9ATCG7R647KKEAW2MUE6W'
    
    main(apiKey, master_apiKey)
    
    # lambda_handler()