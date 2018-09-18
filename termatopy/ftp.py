import ftplib
import io
import pandas as pd
from zipfile import ZipFile

def create_ftp_object(host, user, password):
    '''

    Create an FTP object to interact with FTP transfer protocol
    -----------
    DETAILS
    -----------
    This function is used to access an FTP transfer protocol object upon which methods from the ftplib library can be used
    -----------
    PARAMS
    -----------
    :param host: FTP Host to connect to. EG 'smi.sharefileftp.com'
    :param user: FTP User associated with the FTP host. EG 'smi/michael.lim@blueflag.com.au'
    :param password: Password of the FTP user
    :return: We return an FTP object upon which we can use a variety of methods
    '''
    ftp_object = ftplib.FTP(host=host,
                            user=user,
                            passwd=password)
    return ftp_object


def list_files_ftp(ftp_object, directory):
    '''
    List files within an FTP directory
    -----------
    DETAILS
    -----------
    This function is used to return a list of files from an FTP directory.
    The directory name should be in a format like "Root/Secondary" with no leading or trailing backslashes
    -----------
    PARAMS
    -----------
    :param ftp_object: An FTP Object returned by the create_ftp_object function
    :param directory: A string that
    :return:
    '''
    ftp_directory_list = ftp_object.nlst(directory)
    ftp_directory_list
    return ftp_directory_list


def get_binary_ftp_object(ftp_object, file):
    '''
    Get an object from an ftp directory.

    -----------
    DETAILS
    -----------
    This function is used to return a file from an FTP directory
    The directory name should be in a format like "Root/Secondary/test.txt" with no leading
    -----------
    PARAMS
    -----------
    :param ftp_object: An FTP Object returned by the create_ftp_object function
    :param file: A file within a directory that you would like to read into memory. You must specify the full path.
    :return: binary object of the file that you want read (will require other functions to make useful)
    '''
    binary_object = io.Bytes()
    ftp_object.retrbinary("RETR " + file, binary_object.write)
    return binary_object


def read_file_from_zip(binary_object, zipped_file):
    '''
    Extract a file from a zip archive
    -----------
    DETAILS
    -----------
    Files in FTP folder are often zipped. This is a convenience function to enable the extraction of files from zip archives
    -----------
    PARAMS
    -----------
    :param binary_object: The bytesObject that you've retrieved from the FTP, using get_binary_ftp_object
    :param zipped_file: The file within the zip archive that you've extracted
    :return:
    '''
    zip_object = ZipFile(binary_object)
    return zip_object.read(zipped_file)


def read_delimited_txt_ftp(bytesObject, delimiter):
    '''
    Take a bytes object that you've retrieved from the FTP and convert it into a delimiter text file
    ---------
    DETAILS
    ---------
    Converts a bytesObject into a stringObject, allowing pandas to read a DataFrame from the data that is enclosed
    Currently only written to work with delimited text files
    ---------
    :param bytesObject:
    :param delimiter:
    :return: df, a pandas DataFrame
    '''
    s = str(bytesObject, 'utf-8')
    data = io.StringIO(s)
    df = pd.read_table(data, delimiter = delimiter)
    return df





