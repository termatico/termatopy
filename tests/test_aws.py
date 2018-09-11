import re
import pytest
from termatopy import checkFileType
import numpy as np
import pandas as pd
import json

# Test Definitions
def test_checkFileType_csv():
    csvTest = checkFileType("test.csv")
    assert csvTest == 'csv'

def test_checkFileType_txt():
    txtTest = checkFileType("test.txt")
    assert txtTest == 'txt'
