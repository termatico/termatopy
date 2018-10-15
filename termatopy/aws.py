import pandas as pd
import botocore.config
import boto3
import io
from io import StringIO
import json
import re

def s3ObjectSummaryMetaData(obj):
    summary_dict = {
                'key':obj.key,
                'bucket_name':obj.bucket_name,
                'e_tag':obj.e_tag,
                'last_modified':obj.last_modified,
                'owner':obj.owner['DisplayName'],
                'size':obj.size,
                'storage_class':obj.storage_class
                }
    print(":::Metadata extruded:::")
    return summary_dict

def s3ObjectSummaryMetaDataList(obj_list):
    meta_data_dict_list = [s3ObjectSummaryMetaData(obj) for obj in obj_list]
    return meta_data_dict_list

def checkFileType(file_path):
    if re.search('.csv$', file_path):
        fileType = 'csv'
    elif re.search('.txt$', file_path):
        fileType = 'txt'
    elif re.search('.json$', file_path):
        fileType = 'json'
    return fileType

def fetchS3(access_key, secret, bucket, file, returnObj = False, naFilter = True, delimiter = ","):
    '''
    Fetch a file from an AWS S3 Bucket
    -----------
    DETAILS
    -----------
    This function is used to read files from s3 buckets. Currently it will only work with
    csv files
    -----------
    PARAMS
    -----------
    access_key : Amazon Access Key used to access the specified file
    secret : Amazon Access Secret used to access the specified file
    bucket : The bucket to connect to. This bucket should be specified without the 's3://' prefix e.g. 'my_bucket'
    file : The file within the specified bucket to download. This should also include the
    relative path. Preceeding forward slash can be ommitted. E.g. 'myfolder/myfile.csv'
    returnObj : This will return the object instance of the file rather than reading the file into a dataframe. Useful
    in instances where you don't want to parse the file as a pandas dataframe.
    returnObj: This will return the object instance of the file rather than reading the file into a dataframe. Useful in instances where you don't want to parse the file as a pandas dataframe
    naFilter: Boolean input. Choose how to treat nas. If false your dataframe will return all blank values as NA or NaN. If True, the null values will be returned as blank
    delimiter: Choose the row delimiter to pass. Useful for files like pipe delimited files. Default argument is comm.
    '''
    s3 = boto3.client('s3', aws_access_key_id = access_key, aws_secret_access_key = secret)
    obj = s3.get_object(Bucket = bucket, Key = file)
    fileType = checkFileType(file_path = file)

    if returnObj is True:
        return obj
    elif returnObj is False:
        body = obj['Body'].read()
        if fileType == 'txt':
            data = pd.read_table(io.BytesIO(body), na_filter=naFilter, delimiter=delimiter)
            return data
        elif fileType == 'csv':
            data = pd.read_csv(io.BytesIO(body), na_filter=naFilter)
            return data
        elif fileType == 'json':
            data = json.loads(body)
            return data

def deleteS3(access_key, secret, bucket, file):
    '''
    Delete a file from an S3 Bucket
    -----------
    DETAILS
    -----------
    This function is used to remove files from s3. Note: Once this has been done, there is no way to retrive this file
    -----------
    PARAMS
    -----------
    access_key : Amazon Access Key used to access the specified file
    secret : Amazon Access Secret used to access the specified file
    bucket : The bucket to connect to. This bucket should be specified without the 's3://' prefix e.g. 'my_bucket'
    file : The file within the specified bucket to delete. This should also include the
    relative path. Preceeding forward slash can be ommitted. E.g. 'myfolder/myfile.csv'
    '''
    s3 = boto3.client('s3', aws_access_key_id = access_key, aws_secret_access_key = secret)
    response = s3.delete_object(
        Bucket=bucket,
        Key=file)
    return response

def putS3(access_key, secret, bucket, file, data, includeIndex = True, includePreview = False):
    '''
    Put a file on to an AWS S3 Bucket
    -----------
    DETAILS
    -----------
    This function is used to put files on S3. Currently it will only work with
    csv files
    -----------
    PARAMS
    -----------
    access_key : Amazon Access Key used to access the specified file
    secret : Amazon Access Secret used to access the specified file
    bucket : The bucket to connect to. This bucket should be specified without the 's3://' prefix e.g. 'my_bucket'
    file : The file within the specified bucket to download. This should also include the
    relative path. Preceeding forward slash can be ommitted. E.g. 'myfolder/myfile.csv'
    data : The data object to save. Only rested with Pandas DataFrames currently.
    '''
    try:
        s3 = boto3.client('s3', aws_access_key_id = access_key, aws_secret_access_key = secret)
        fileType = checkFileType(file)
        if fileType in ['csv', 'txt', 'text']:
            csv_buffer = StringIO()

            data.to_csv(csv_buffer, index = includeIndex)
            obj = s3.put_object(Bucket = bucket, Key = file, Body = csv_buffer.getvalue())

        elif fileType == "json":
            obj = s3.put_object(Bucket = bucket, Key = file, Body = json.dumps(data))

        output = {
            "output" : {
                "bucket" : bucket,
                "file" : file
            }
        }

        if includePreview:
            csv_buffer = StringIO()
            previewData = data.head(100).to_csv(csv_buffer, index = includeIndex)
            previewName = re.sub('.' + fileType + '$', "_PREVIEW." + fileType, file)
            obj = s3.put_object(Bucket = bucket, Key = previewName, Body = csv_buffer.getvalue())
            output.update({
                "preview" : {
                    "bucket" : bucket,
                    "file" : previewName
                }
            })
        return output
    except Exception as e:
        raise Exception(str(e))

def listFiles(access_key, secret, bucket, folder = '', startAfter = '', endswith = None):
    '''
    List all files in a bucker or folder on S3
    -----------
    DETAILS
    -----------
    This function is used to list files in an S3 Bucket or within a folder on S3
    -----------
    PARAMS
    -----------
    access_key : Amazon Access Key used to access the specified file
    secret : Amazon Access Secret used to access the specified file
    bucket : The bucket to connect to. This bucket should be specified without the 's3://' prefix e.g. 'my_bucket'
    folder : The folder structure (e.g. myFolder/mySubFolder)
    startAfter : Load objects that appear after this key only
    '''
    try:
        s3 = boto3.client('s3', aws_access_key_id = access_key, aws_secret_access_key = secret)
        response = s3.list_objects_v2(Bucket = bucket, Prefix = folder, StartAfter = startAfter)
        allKeys = []
        for i in range(0, len(response['Contents'])):
            allKeys.append(response['Contents'][i]['Key'])

        if endswith != None:
            allKeys = list(filter(lambda x: str(x).endswith(endswith), allKeys))
        return allKeys
    except Exception as e:
        raise Exception(str(e))


def extractBucketName(location):
    '''
    Extract bucket name and file from s3:// URI
    '''
    params = location.split("//")[1]
    bucket = params.split("/")[0]
    path = params.split("/", maxsplit = 1)[1:]
    return bucket, path

def invokeLambda(accessKey, secret, arn, payload, region = 'ap-southeast-2', maxRetries = 0):
    try:
        cfg = botocore.config.Config(retries={'max_attempts': maxRetries})
        client = boto3.client('lambda', aws_access_key_id = accessKey, aws_secret_access_key = secret, region_name = region, config=cfg)
        res = client.invoke(FunctionName = arn, Payload = json.dumps(payload), InvocationType = 'Event')
        return res
    except Exception as e:
        return str(e)
