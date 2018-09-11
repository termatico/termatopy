import pytz
from datetime import datetime as dt
ausTZ = pytz.timezone("Australia/Melbourne")

def output(data = [], logs = [], errors = []):
    '''
    Return a Termatico Template Compliant data structure
    -----------
    DETAILS
    -----------
    Termatico expects a specific data format in order to use it as part
    of a template. This structure includes a data list, logs list and errors list.
    The function you write will be responsible for passing the correct data to each
    list. This function is normally used in the final return of a function.
    '''
    formattedData = {
        'data': data,
        'errors': errors,
        'logs' : logs
    }

    return formattedData

class Logger:
    '''
    logData = Logger()
    logData.add_log("info", "Test Log Message One")
    logData.add_log("info", "Test Log Message Two")
    list(logData.logs)
    '''
    def __init__(self):
        self.logs = []

    def add_log(self, type, message):
        newLog = {
                "timestamp" : dt.today().astimezone(ausTZ).strftime("%Y-%m-%dT%H:%m:%s"),
                "type" : str(type),
                "message" : str(message)
               }
        self.logs.append(newLog)
