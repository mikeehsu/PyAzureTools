import os
import sys
import math
import json
import getopt
import pandas as pd
import requests
from azure.identity import DefaultAzureCredential

global url
global headers
global guid

def test_purview_access(purview_account_name):
    response = requests.get(f"https://{purview_account_name}.purview.azure.com/?api-version=2019-11-01-preview", headers=headers)
    if response.status_code == 200:
        return(True)
    else:
        print(f"{response.status_code} - {response.text}")
        return(False)


def create_schema(server_name, instance_name, database_name, schema_name):
    global guid

    entities = []

    # send schema creation request
    # Send database creation request
    guid = guid-1
    database_guid = str(guid)

    database = {}
    database['typeName'] = "mssql_db"
    database['guid'] = database_guid
    database['attributes'] = {}
    database['attributes']['qualifiedName'] = f"mssql://{server_name}/{instance_name}/{database_name}"
    database['attributes']['name'] = database_name
    database['relationshipAttributes'] = {}
    entities.append(database)

    guid = guid-1
    schema_guid = str(guid)

    schema = {}
    schema['typeName'] = "mssql_schema"
    schema['guid'] = schema_guid
    schema['attributes'] = {}
    schema['attributes']['qualifiedName'] = f"mssql://{server_name}/{instance_name}/{database_name}/{schema_name}"
    schema['attributes']['name'] = schema_name
    schema['relationshipAttributes'] = {}
    entities.append(schema)

    data = {}
    data['entities'] = entities
    json_data = json.dumps(data)

    response = requests.post(f"{url}/entity/bulk?isOverwrite=true", headers=headers, data=json_data)
    if (response.status_code == 201 or response.status_code == 200):
        result = json.loads(response.text)
        if ('mutatedEntities' in result):
            print(f"{server_name}/{instance_name}/{database_name}/{schema_name} (guid={result['guidAssignments'][schema_guid]})...schema updated successfully")
        else:
            print(f"{server_name}/{instance_name}/{database_name}/{schema_name} (guid={result['guidAssignments'][schema_guid]})...no changes needed")
        return(result['guidAssignments'][schema_guid])

    else:
        print(f"{server_name}/{instance_name}/{database_name}/{schema_name} creation failed:", response.status_code)
        print(response.text)
        return("")

def load_glossary_terms(glossary_name):
    global url
    global headers

    response = requests.get(f"{url}/glossary", headers=headers)
    if (response.status_code == 200):
        glossaries = json.loads(response.text)

    else:
        print("Glossary terms retrieval failed:", response.status_code)
        print(response.text)
        return([])

    for glossary in glossaries:
        if (glossary['name'] == glossary_name):
            terms = glossary['terms']
            # print(json.dumps(glossary_terms))
            return(terms)

    return([])

def get_glossary_guid(glossary_terms, term):

    if term == "" or type(term) is float:
        return("")

    for glossary in glossary_terms:
        if (glossary['displayText'].endswith(term)):
            return(glossary['termGuid'])

    return("")


#
# MAIN
#
if __name__ == "__main__":

    # get parameters
    sys.argv[1:]

    input_file = ""
    purview_account_name = ""
    server_name = ""
    server_header = ""
    instance_name = ""
    instance_header = ""
    database_name = ""
    database_header = ""
    schema_name = ""
    schema_header = ""
    table_header = "TableName"
    column_header = "ColumnName"
    type_header = ""
    description_header = ""
    glossary_header = ""
    qualified_name_header = ""

    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["file=", "purview=", "server=", "server_header=", "instance=", "instance_header", "database=", "database_header", "schema=", "schema_header", "table=", "column=", "type=", "description=", "glossary=", "glossary_header=", "qualified="])
    except getopt.GetoptError:
        print('create_purview_sql_from_csv.py --file <inputfile> --purview <purviewaccount> --server <servername> --server_header <serverheader> --instance <instancename> --instance_header <instanceheader> --database <databasename> --database_header <databaseheader> --schema <schemaname> --schema_header <schemaheader> --table <tableheader> --column <columnheader> --type <typeheader> --description <descriptionheader> --glossary --glossary_header --qualified <qualifiednameheader>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print('create_purview_sql_from_csv.py --file <inputfile> --purview <purviewaccount> --server <servername> --server_header <serverheader> --instance <instancename> --instance_header <instanceheader> --database <databasename> --database_header <databaseheader> --schema <schemaname> --schema_header <schemaheader> --table <tableheader> --column <columnheader> --type <typeheader> --description <descriptionheader> --glossary --glossary_header --qualified <qualifiednameheader>')
            sys.exit()
        elif opt in ("--file"):
            input_file = arg
        elif opt in ("--purview"):
            purview_account_name = arg
        elif opt in ("--server"):
            server_name = arg
        elif opt in ("--server_header"):
            server_header = arg
        elif opt in ("--instance"):
            instance_name = arg
        elif opt in ("--instance_header"):
            instance_header = arg
        elif opt in ("--database"):
            database_name = arg
        elif opt in ("--database_header"):
            database_header = arg
        elif opt in ("--schema"):
            schema_name = arg
        elif opt in ("--schema_header"):
            schema_header = arg
        elif opt in ("--table"):
            table_header = arg
        elif opt in ("--column"):
            column_header = arg
        elif opt in ("--type"):
            type_header = arg
        elif opt in ("--description"):
            description_header = arg
        elif opt in ("--glossary"):
            glossary_name = arg
        elif opt in ("--glossary_header"):
            glossary_header = arg
        elif opt in ("--qualified"):
            qualified_name_header = arg

    # display all parameters
    # print("=====Parameters=====")
    # print(f"Input file: {input_file}")
    # print(f"Purview account name: {purview_account_name}")
    # print(f"Server name: {server_name}")
    # print(f"Instance name: {instance_name}")
    # print(f"database name: {database_name}")
    # print(f"schema name: {schema_name}")
    # print(f"Table name header: {table_header}")
    # print(f"Column name header: {column_header}")
    # print(f"Type header: {type_header}")
    # print(f"Description header: {description_header}")
    # print(f"Qualified name header: {qualified_name_header}")

    # validate required parameters
    if (input_file == ""):
        print("Input file is required")
        sys.exit(2)

    if (purview_account_name == ""):
        print("Purview account name is required")
        sys.exit(2)

    if (server_name == "" and server_header == ""):
        print("server_name or server_header is required")
        sys.exit(2)

    if (instance_name == "" and instance_header == ""):
        print("instance_name or instance_header is required")
        sys.exit(2)

    if (database_name == "" and database_header == ""):
        print("database_name or database_header is required")
        sys.exit(2)

    if (table_header == ""):
        print("table_header is required")
        sys.exit(2)

    if (column_header == ""):
        print("column_header is required")
        sys.exit(2)

    if (glossary_name == "" and glossary_header != "") or (glossary_name != "" and glossary_header == ""):
        print("glossary and glossary_header must be used together")
        sys.exit(2)


    # validate conflicting parameters
    if (server_name != "" and server_header != ""):
        print("server_name and server_header cannot be used together")
        sys.exit(2)

    if (instance_name != "" and instance_header != ""):
        print("instance_name and instance_header cannot be used together")
        sys.exit(2)

    if (database_name != "" and database_header != ""):
        print("database_name and database_header cannot be used together")
        sys.exit(2)


    # read input file
    data = pd.read_csv(input_file)

    # validate input file to make sure it has the required/specified columns
    if (server_header != "" and server_header not in data.columns):
        print(f"Server name header '{server_header}' not found in input file")
        sys.exit(2)

    if (instance_header != "" and instance_header not in data.columns):
        print(f"Instance name header '{instance_header}' not found in input file")
        sys.exit(2)

    if (database_header != "" and database_header not in data.columns):
        print(f"Database name header '{database_header}' not found in input file")
        sys.exit(2)

    if (table_header not in data.columns):
        print(f"Table name header '{table_header}' not found in input file")
        sys.exit(2)

    if (column_header not in data.columns):
        print(f"Column name header '{column_header}' not found in input file")
        sys.exit(2)

    if ((type_header != "") and (type_header not in data.columns)):
        print(f"Type header '{type_header}' not found in input file")
        sys.exit(2)

    if ((description_header != "") and (description_header not in data.columns)):
        print(f"Description header '{description_header}' not found in input file")
        sys.exit(2)

    if ((glossary_header != "") and (glossary_header not in data.columns)):
        print(f"Glossary name header '{glossary_header}' not found in input file")
        sys.exit(2)

    if ((qualified_name_header != "") and (qualified_name_header not in data.columns)):
        print(f"Qualified name header '{qualified_name_header}' not found in input file")
        sys.exit(2)


    # Authenticate against your Atlas server
    try:
        credential = DefaultAzureCredential()
        access_token = credential.get_token("https://purview.azure.net")
    except:
        print("Unable to get & verify Azure credentials. Please login with 'az login' and try again.")
        sys.exit(2)


    # initialize global variables
    guid = -1000
    url = f"https://{purview_account_name}.purview.azure.com/catalog/api/atlas/v2"
    headers = {
        "Authorization": f"Bearer {access_token.token}",
        "Content-Type": "application/json"
    }
    # print(f"URL: {url}")
    # print(headers)
    if (not test_purview_access(purview_account_name)):
        print(f"Unable to verify purview access. Please make sure you are authenticated and have permission to Purview ({purview_account_name}) and try again.")
        sys.exit(2)

    # load glossary terms
    glossary_terms = []
    if (glossary_header != ""):
        glossary_terms = load_glossary_terms(glossary_name=glossary_name)
        if (len(glossary_terms) == 0):
            print(f"No glossary terms for Glossary '{glossary_name}' found")
            sys.exit(2)

    # create database entity
    schema_guid = create_schema(server_name=server_name, database_name=database_name, instance_name=instance_name, schema_name=schema_name)
    if (schema_guid == ""):
        sys.exit(2)

    # create table groups
    tables = data.groupby(table_header)
    table_names = tables.groups.keys()

    # create table entities
    for table_name in table_names:
        print(f"{table_name}...", end='', flush=True)
        if (table_name.find(',') != -1):
            print(f"\r{table_name}...invalid. Please fix and try again.")
            continue

        # add database
        # database = {}

        # iniitalize entities structure
        entities = []

        # add tables
        guid = guid-1
        table_guid = guid
        table = {}
        table['typeName'] = "mssql_table"
        table['guid'] = str(guid)
        table['attributes'] = {}
        table['attributes']['name'] = table_name

        # add qualified name
        if (qualified_name_header == ""):
            table['attributes']['qualifiedName'] = f"mssql://{server_name}/{instance_name}/{database_name}/{schema_name}/{table_name}"
        else:
            table['attributes']['qualifiedName'] = data.loc[data[table_header] == table_name and data[column_header] == ""][qualified_name_header]

        # add table relationship to database
        table['relationshipAttributes'] = {}
        table['relationshipAttributes']['dbSchema'] = {}
        table['relationshipAttributes']['dbSchema']['guid'] = schema_guid

        entities.append(table)

        # create columns
        for index, row in tables.get_group(table_name).iterrows():
            # print(f"{row[column_header]}, {row[type_header]}")

            # create main column entity
            guid = guid-1
            column = {}
            column['typeName'] = "mssql_column"
            column['guid'] = str(guid)
            column['attributes'] = {}
            column['attributes']['name'] = row[column_header]

            # add qualified name
            if (qualified_name_header == ""):
                column['attributes']['qualifiedName'] = f"mssql://{server_name}/{instance_name}/{database_name}/{schema_name}/{table_name}#{row[column_header]}"
            else:
                column['attributes']['qualifiedName'] = row[qualified_name_header]

            # add optional attributes
            if (type_header != ""):
                column['attributes']['data_type'] = row[type_header]

            if (description_header != ""):
                column['attributes']['description'] = row[description_header]

            # add relationship of column to table
            column['relationshipAttributes'] = {}
            column['relationshipAttributes']['table'] = {}
            column['relationshipAttributes']['table']['guid']= table['guid']

            # add glossary term
            if (glossary_header != "" and row[glossary_header] != ""):
                glossary_guid = get_glossary_guid(glossary_terms, row[glossary_header])
                if (glossary_guid != ""):
                    # assign glossary term to column
                    meaning = {}
                    meaning['guid'] = glossary_guid
                    column['relationshipAttributes']['meanings'] = [meaning]

            entities.append(column)

        # create structure for API
        data = {}
        data['entities'] = entities
        json_body = json.dumps(data)
        # print(json_body)

        # send table creation to purview
        response = requests.post(f"{url}/entity/bulk?isOverwrite=true", headers=headers, data=json_body)
        if response.status_code == 200:
            if 'mutatedEntities' in response.json():
                print(f"\r{table_name}...updated successfully")
            else:
                print(f"\r{table_name}...no changes needed")

        else:
            print(f"\n{table_name}...failed. Error: {response.status_code} - {response.reason} - {response.text}")


