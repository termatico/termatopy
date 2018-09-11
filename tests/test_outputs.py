import pytest
from termatopy import output
import numpy as np
import pandas as pd
import json

# Test Definitions
def test_output_data():
    testData = pd.DataFrame(["Paul McCartney", "John Lennon", "George Harrisn", "Ringo Starr"], columns = ["Name"]).to_json()

    outputResult = output(data = testData, logs = [], errors = [])

    dataLength = len(pd.DataFrame(json.loads(outputResult['data'])))
    assert dataLength == 4

def test_output_logs():
    testLogs = pd.DataFrame(["This is a test log", "this is a second test log"], columns = ["log"]).to_json()

    outputResult = output(data = [], logs = testLogs, errors = [])

    logLength = len(json.loads(outputResult['logs'])['log'])

    assert logLength == 2


def test_output_errors():
    testLogs = pd.DataFrame(["This is a test error", "this is a second test", "this is a third test error"],  columns = ["log"]).to_json()

    outputResult = output(data = [], logs = testLogs, errors = [])

    logLength = len(json.loads(outputResult['logs'])['log'])

    assert logLength == 3
