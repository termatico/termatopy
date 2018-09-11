import json
import os
from dateutil.parser import parse

def source(location = None):
    if location == None:
        location = str(os.getenv("HOME")) + "/.pyprofile"
    text = open(location).read()
    config = json.loads(text)
    return config
    
class profile:
    '''
    env = profile()
    env.read()
    env.add({"hello" : {"test" : "testOne"}})
    env.delete("hello")
    env.profile

    profile is a way to manage keys, secrets and other information that you want
    to store for your Python environment. Reason for this came from no real alternative
    that works in a similar way to the way R works with its .Rprofile.

    Basics of this class are:
    create: Add a new profile. All profiles are json documents
    add: Add a key value pair, or nested object to your profile
    delete: Remove key from profile
    read: Read profile

    If you have a profile in your HOME directory it will default to using that. Note: Mac is the only OS
    supported for this.
    '''
    def __init__(self):
        self.profile = {}

    def create(self, config = {}, location = None):
        if location == None:
            location = str(os.getenv("HOME")) + "/pyprofile.json"
        assert type(config) == dict
        if os.path.isfile(location):
            raise Exception(location + " is already a valid profile. Please choose another location or use `add` to add to existing profile.")

        self.profile.update(config)
        with open(location, 'w') as outfile:
            json.dump(self.profile, outfile)
        return location

    def read(self, location = None):
        if location == None:
            location = str(os.getenv("HOME")) + "/pyprofile.json"
        text = open(location).read()
        config = json.loads(text)
        self.profile.update(config)

    def add(self, config, location = None):
        if location == None:
            location = str(os.getenv("HOME")) + "/pyprofile.json"
        text = open(location).read()
        oldconfig = json.loads(text)
        self.profile.update(oldconfig)
        self.profile.update(config)
        with open(location, 'w') as outfile:
            json.dump(self.profile, outfile)

    def delete(self, config, location = None):
        if location == None:
            location = str(os.getenv("HOME")) + "/pyprofile.json"
        text = open(location).read()
        oldconfig = json.loads(text)
        self.profile.update(oldconfig)
        self.profile.pop(config)
        with open(location, 'w') as outfile:
            json.dump(self.profile, outfile)


def is_date(string):
    try:
        parse(string)
        return True
    except ValueError:
        return False


def is_string(string):
    try:
        string.isalpha()
        return True
    except AttributeError:
        return False


def is_numeric(string):
    try:
        float(string)
        return True
    except ValueError:
        return False


def check_false(series):
    if False in series:
        return False
    else:
        return True


def check_column_type(field, data, type_dict):
    '''
    Check types of columns within a DataFrame to see if they are the types that you expect
    This function is designed to loop over a list of field names that you want checked
    It will only enforce checking over three types : date, string, numeric
    Example of a loop using this function [check_column_type(each, data=test_data, type_dict=test_dict) for each in fields]
    :param field: the column of data you want to check
    :param data: the DataFrame in which your data exists
    :param type_dict: key value pair with the field name and the data type you what to validate. eg {field_name:string}
    :return:
    '''
    series_field = data[field]
    type_check = type_dict[field]
    if type_check == 'date':
        date_checker = [is_date(each) for each in series_field]
        bool_check = check_false(date_checker)
    elif type_check == 'string':
        string_checker = [is_string(each) for each in series_field]
        bool_check = check_false(string_checker)
    elif type_check == 'numeric':
        numeric_checker = [is_numeric(each) for each in series_field]
        bool_check = check_false(numeric_checker)
    else:
        raise Exception(f"TYPE::{type} in FIELD::{field} could not be validated. Check your arguments")
    return field, bool_check
