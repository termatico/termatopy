import warnings
warnings.filterwarnings("ignore")
import ast
import psycopg2 as ps
from psycopg2 import sql
from psycopg2.extras import Json
import pymongo as pm
import pandas as pd
import numpy
import boto3
from bson.json_util import loads as bson_loads
from .aws import extractBucketName
from .aws import fetchS3

def queryPostgres(host, port, user, password, database, query):
    '''
    Submit a blocking query to Postgres
    -----------
    DETAILS
    -----------
    This function will send a query to a Postgres compliant database (including Amazon Redshift and Amazon Aurora).
    The query will wait for a response with the data which means any queries will need to resolve within 5 minutes if
    invoking on as a Lambda Function (or 30 seconds if invoking the lambda through HTTP).
    -----------
    PARAMS
    -----------
    host : The hostname of the database to connect to
    port : The port that accepts connections
    user : username that has permission to execute queries
    password : The password for authentication
    database : The database one the Postgres instance to run the query against
    query : The query to execute
    '''
    try:
        conn = ps.connect("dbname='" + database + "' user='" + user + "' host='" + host + "' port='" + port + "' password='" + password + "'")
        cur = conn.cursor()
        cur.execute(query)

        columns = []
        for i in range(len(cur.description)):
            columns.append(cur.description[i].name)
            pass

        rows = cur.fetchall()
        data = pd.DataFrame(rows)
        data.columns = columns
        conn.close()
        return data
    except ValueError as e:
         raise Exception("ValueError: Most likely no rows were returned from database.")

def runQueryPostgres(host, port, user, password, database, query):
    try:
        conn = ps.connect("dbname='" + database + "' user='" + user + "' host='" + host + "' port='" + port + "' password='" + password + "'")
        curs = conn.cursor()
        curs.execute(query)
        conn.commit()
        conn.close()
        pass
    except Exception as e:
        raise Exception(str(e))

def queryMongo(host, port, database, collection, query):
    '''
    -----------
    PARAMS
    -----------
    host : The hostname of the database to connect to
    port : The port that accepts connections
    user : username that has permission to execute queries
    password : The password for authentication
    database : The database one the Postgres instance to run the query against
    query : The query to execute
    '''
    try:
        mongoURL = 'mongodb://' + host + ':' + port
        client = pm.MongoClient(mongoURL)
        db = client.get_database(database)
        col = db.get_collection(collection)

        curs = col.find(bson_loads(query))
        data = pd.DataFrame(list(curs))
        return data
    except ValueError as e:
         raise Exception("ValueError: Most likely no rows were returned from database.")

def createFieldReplacement(repeats):
        repeats = repeats - 1
        fieldReplacement = "%s, "
        fieldReplacement = fieldReplacement * repeats
        fieldReplacement = fieldReplacement + "%s"
        fieldReplacement = "(" + fieldReplacement + ")"
        return fieldReplacement


def cleanUpString(text, strip_chars=[], replace_extras={}):
    # Credit for this code goes to: https://codereview.stackexchange.com/users/114734/double-j
    # Taken from this URL: https://codereview.stackexchange.com/questions/139549/python-string-clean-up-function-with-optional-args
    clean_up_items = {'\n': ' ', '\r': ' ', '\t': ' ', '  ': ' '}
    clean_up_items.update(replace_extras)

    text = text.strip()

    change_made = True
    while change_made:
        text_old = text
        for x in strip_chars:
            while text.startswith(x) or text.endswith(x):
                text = text.strip(x).strip()

        for key, val in clean_up_items.items():
            while key in text:
                text = text.replace(key, val)

        change_made = False if text_old == text else True

    return text.strip()

def insertToPostgres(host, port, username, password, database, table, data, columns, upsertPrimaryKey = None):
    try:
        data = data.where((pd.notnull(data)), None)
        rowsToInsert = len(data)
        fieldReplacement = createFieldReplacement(len(data.columns))
        conn = ps.connect("dbname='" + database + "' user='" + username + "' host='" + host + "' port='" + port + "' password='" + password + "'")
        cur = conn.cursor()
        allRowSql = bytes(b"INSERT INTO " + table.encode() + b" (" + cleanUpString(str(data.columns.values.tolist()), ["[", "]", "'"], {"'" : ""}).encode() + b") VALUES ")

        for i in range(rowsToInsert):
            row = data.iloc[i].values.tolist()
            if i == (rowsToInsert - 1):
                rowSql = cur.mogrify(fieldReplacement, (row))
            else:
                rowSql = cur.mogrify(fieldReplacement, (row)) + b","

            allRowSql = allRowSql + rowSql

        if upsertPrimaryKey != None:
            upsertPrimaryKey = str(upsertPrimaryKey).replace("[", "").replace("]", "").replace("'", "")

            baseUpsert = b" ON CONFLICT (" + upsertPrimaryKey.encode() + b") DO UPDATE SET "

            allRowSql = allRowSql + baseUpsert

            for i in range(len(data.columns)):
                if i == (len(data.columns) - 1):
                    columnUpsert = data.columns.values.tolist()[i].encode() + b" = EXCLUDED." + data.columns.values.tolist()[i].encode()
                    allRowSql = allRowSql + columnUpsert
                else:
                    columnUpsert = data.columns.values.tolist()[i].encode() + b" = EXCLUDED." + data.columns.values.tolist()[i].encode() + b","
                    allRowSql = allRowSql + columnUpsert

        cur.execute(allRowSql)
        conn.commit()
        conn.close()
        results = {"columns" : len(data.columns), "rows" : len(data)}
        return results
    except Exception as e:
        conn.close()
        raise Exception(str(e))


def insertToPostgres2(host, username, password, database, table, data, column_types, port=5432, schema="public",
                      page_size=100, unique_key_list=[]):
    conn = ps.connect(host=host, port=port, database=database, user=username, password=password)
    cur = conn.cursor()

    progress = 0
    col_names = list(data)

    for index, value in data.iterrows():
        value_list = list()
        column_list = list()

        for col_name in col_names:
            column_list.append(col_name.lower())
            value_list.append(convertColumnType(col_name, value, column_types))

        insert_query = insertToPostgresSqlPlain(schema, table, column_list, value_list, unique_key_list)

        cur.execute(insert_query, value_list)
        progress += 1

        if progress % page_size == 0:
            conn.commit()

    conn.commit()
    conn.close()
    output = {"columns" : len(data.columns), "rows" : len(data)}
    return pd.DataFrame(output, index = [0])


def insertToPostgresSqlPlain(relation, target, column_list, value_list, unique_key_list):
    insert_query = "INSERT INTO {relation}.{target} ({columns_insert}) VALUES ({value_insert})" + (
        " ON CONFLICT DO NOTHING" if (len(
            unique_key_list) == 0) else " ON CONFLICT ({pk}) DO UPDATE SET ({columns_update}) = ({value_update})")
    kwargs = dict()

    kwargs["relation"] = relation
    kwargs["target"] = target
    kwargs["columns_insert"] = ", ".join(column_list)
    kwargs["value_insert"] = ", ".join(["%s"] * len(value_list))

    if len(unique_key_list) > 0:
        kwargs["pk"] = ", ".join(unique_key_list)

        kwargs["columns_update"] = ", ".join(
            [column for column in column_list if column not in unique_key_list]
        )
        kwargs["value_update"] = ", ".join(
            [("EXCLUDED." + column) for column in column_list if column not in unique_key_list]
        )
    return insert_query.format(**kwargs)


def convertColumnType(column, values, column_types):
    column_type = column_types[column]
    column_value = values.get(column)

    if column_type == "json":
        if isinstance(column_value, str):
            return Json(ast.literal_eval(column_value))
        else:
            return Json(column_value)
    elif column_type == "text":
        return str(column_value)
    elif column_type == "int":
        return int(column_value)
    else:
        raise Exception("Unknown column [%s] of type [%s]" % (column, column_type))


def getExecutionStatus(executionId, client):
    execution = client.get_query_execution(QueryExecutionId = executionId)
    outputLocation = execution['QueryExecution']['ResultConfiguration']['OutputLocation']
    status = execution['QueryExecution']['Status']['State']
    reason = execution.get("QueryExecution", {}).get("Status", {}).get("StateChangeReason", None)
    return status, outputLocation, reason

def queryAthena(access_key, access_secret, query, resultLocation):
    try:
        client = boto3.client('athena', aws_access_key_id = access_key, aws_secret_access_key = access_secret)
        queryRequest = client.start_query_execution(QueryString = query, ResultConfiguration = {'OutputLocation' : resultLocation})

        executionStatus = getExecutionStatus(str(queryRequest['QueryExecutionId']), client)
        status = executionStatus[0]

        while status == 'RUNNING':
            executionStatus = getExecutionStatus(str(queryRequest['QueryExecutionId']), client)
            status = executionStatus[0]

        if status == 'FAILED':
            raise Exception(executionStatus[2])

        else:
            resultDataLocation = extractBucketName(executionStatus[1])
            resultData = fetchS3(access_key, access_secret, resultDataLocation[0], resultDataLocation[1][0])
        return resultData
    except Exception as e:
        raise Exception(str(e))
