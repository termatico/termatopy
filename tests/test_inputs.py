import pytest
from termatopy import output
from termatopy import source
import numpy as np
import pandas as pd
import json

# import os
# os.chdir("/Users/mitchelllisle/Documents/termatico/termatopy/tests")

# Test Definitions
def test_output_data():
    testData = pd.DataFrame(["Paul McCartney", "John Lennon", "George Harrisn", "Ringo Starr"], columns = ["Name"]).to_json()

    outputResult = output(data = testData, logs = [], errors = [])

    dataLength = len(pd.DataFrame(json.loads(outputResult['data'])))
    assert dataLength == 4

def test_source():
    profile = source("tests/.pyprofile")
    assert profile["testKey"] == "testValue"
