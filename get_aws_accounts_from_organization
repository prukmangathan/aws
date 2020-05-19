import boto3
import json
import os
import datetime

Organizations = boto3.client('organizations')
Config = boto3.client('config')
dynamodb = boto3.client('dynamodb')
dynamodb_table = os.environ["DYNAMODB_TABLE_NAME"]
AGG_NAME = os.environ["AGGREGATOR_NAME"]
RES_TYP = os.environ["RESOURCE_TYPES"]

def lambda_handler(event, context):
    all_accounts=Organizations.list_accounts()
    while True:
      for account_info in all_accounts['Accounts']:
          response = UpdateDB(account_info)
          print (response)
      if 'NextToken' in all_accounts:
    	  all_accounts = Organizations.list_accounts(NextToken=all_accounts['NextToken'])
      else:
    	  break

def UpdateDB(Acc_Info):
    data_dict = format_data(Acc_Info)
    Update_Table = dynamodb.put_item(
        TableName=dynamodb_table,
        Item={
               "AccountID"          : { "S": data_dict["AccountID"]           }, 
               "Date"               : { "S": data_dict["Date"]                }, 
               "Email"              : { "S": data_dict["Email"]               }, 
               "Name"               : { "S": data_dict["Name"]                }, 
               "OU-id"              : { "S": data_dict["OU-id"]               }, 
               "OU-name"            : { "S": data_dict["OU-name"]             },
               "Organization-ID"    : { "S": data_dict["Organization-ID"]     }, 
               "Organization-parent": { "S": data_dict["Organization-parent"] }, 
               "Status"             : { "S": data_dict["Status"]              }, 
               "Used Services"      : { "SS": data_dict["Used Services"]      },
    	       "OU-path"            : { "S": data_dict['OU-path']             }
    	     }
    )
    return (Update_Table)
    #print("Updating dynamodb table for account " + data_dict["AccountID"])

def format_data(Mydata):
    Mydict = {}
    fullDate = Mydata['JoinedTimestamp']
    Date = str(fullDate).split(' ')[0]
    aws_account_id = Mydata['Id']
    uServ = []
    Mydict["AccountID"] = aws_account_id
    Mydict["Status"] = Mydata['Status']
    Mydict["Name"] = Mydata['Name']
    Mydict["Email"] = Mydata['Email']
    Mydict["Date"] = Date
    all_services = get_used_services_from_config(AGG_NAME,RES_TYP)
    if aws_account_id in all_services.keys():
       for lIst in all_services[aws_account_id]:
           uServ.append(str(lIst))
    else:
       uServ.append("null")
    Mydict["Used Services"] = uServ
    orgdetails = get_org_id(aws_account_id)
    Mydict["Organization-ID"] = orgdetails['Org_id']
    Mydict["OU-id"] = orgdetails['Org_unit_info']['Parent_id']
    Mydict["OU-name"] = orgdetails['Org_unit_info']['org_unit_name']
    Mydict["Organization-parent"] = orgdetails['Org_parent']
    ou_path = get_ou_path(aws_account_id)
    Full_ou_path = "Root" + "/" + ou_path
    Mydict['OU-path'] = Full_ou_path
    return (Mydict)

def get_used_services_from_config(ag_na,re_ty):
    Resourcetypes = re_ty.split(',')
    used_services = {}
    for Resourcetype in Resourcetypes:
        ladr = Config.list_aggregate_discovered_resources(
                      ConfigurationAggregatorName=ag_na,
                      ResourceType=Resourcetype
               )
        for resource in ladr["ResourceIdentifiers"]:
            acc_id = resource['SourceAccountId']
            #print (acc_id)
            rt = resource["ResourceType"]
            rType = rt.split('::')[1]
            if acc_id not in used_services:
               used_services[acc_id] = []
            if acc_id in used_services:
               if rType not in used_services[acc_id]:
                  used_services[acc_id].append(rType)
    return (used_services)
    
def get_org_id(aws_acc_id):
    org_Info = {}
    desc_Org = Organizations.describe_organization()
    org_Info["Org_id"] = desc_Org['Organization']['Id']
    ListParents = Organizations.list_parents(ChildId=aws_acc_id)
    for Parent in ListParents['Parents']:
        Parent_id = Parent['Id']
    desc_org_units = Organizations.describe_organizational_unit(OrganizationalUnitId=Parent_id)
    org_unit_name = desc_org_units['OrganizationalUnit']['Name']
    org_Info["Org_unit_info"] = {'Parent_id': Parent_id, 'org_unit_name': org_unit_name}
    org_Info["Org_parent"] = Organizations.list_roots()['Roots'][0]['Id']
    return (org_Info)

def get_ou_path(UnitId):
    oupath = None
    while True:
        parent_info = get_parent(UnitId)
        if parent_info['parent_name'] != "Root":
           if oupath != None:
               oupath = parent_info['parent_name'] + "/" + oupath
           else:
               oupath = parent_info['parent_name']
           UnitId = parent_info['Parent_Id']
        else:
            break
    return (oupath)

def get_parent(UID):
    try:
        list_parents = Organizations.list_parents(ChildId=UID)
        for parent in list_parents['Parents']:
            Parent_Id = parent['Id']
            Parent_type = parent['Type']
        if Parent_type != 'ROOT':
           parent_name = get_unit_name(Parent_Id)
           if parent_name == None:
              parent_name = "Root"
        else:
           parent_name = "Root"
        return ({'Parent_Id': Parent_Id, 'parent_name': parent_name})
    except:
        print ("root account would not have parents")

def get_unit_name(ouId):
    try:
       desc_ou = Organizations.describe_organizational_unit(OrganizationalUnitId=ouId) 
       ou_name = desc_ou['OrganizationalUnit']['Name']
       return (ou_name)
    except:
       return None
