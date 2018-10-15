import boto3

from termatopy import checkFileType
import termatopy as tpy
from unittest.mock import MagicMock
from unittest.mock import Mock


# Test Definitions
def test_checkFileType_csv():
    csvTest = checkFileType("test.csv")
    assert csvTest == 'csv'


def test_checkFileType_txt():
    txtTest = checkFileType("test.txt")
    assert txtTest == 'txt'


def test_check_file_type_json():
    json_test = checkFileType("test.json")
    assert json_test == 'json'


def test_fetch_s3_txt():
    mock_s3_client = Mock()
    mock_body = Mock()
    expected = bytearray("{}", "utf-8")

    boto3.client = MagicMock(return_value=mock_s3_client)
    mock_s3_client.get_object = MagicMock(return_value={"Body": mock_body})
    mock_body.read = MagicMock(return_value=expected)

    actual = tpy.fetchS3("access_key", "secret_key", "bucket", "file.txt")

    from pandas import DataFrame
    assert isinstance(actual, DataFrame)

    assert expected == bytearray(actual.to_json(orient="index"), "utf-8")


def test_fetch_s3_csv():
    mock_s3_client = Mock()
    mock_body = Mock()
    expected = bytearray("{}", "utf-8")

    boto3.client = MagicMock(return_value=mock_s3_client)
    mock_s3_client.get_object = MagicMock(return_value={"Body": mock_body})
    mock_body.read = MagicMock(return_value=expected)

    actual = tpy.fetchS3("access_key", "secret_key", "bucket", "file.csv")

    from pandas import DataFrame
    assert isinstance(actual, DataFrame)

    assert expected == bytearray(actual.to_json(orient="index"), "utf-8")
