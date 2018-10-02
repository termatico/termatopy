from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import Mock
import ftplib
import io
from termatopy.ftp import ftpCreateInstance
from termatopy.ftp import ftpListFiles
from termatopy.ftp import ftpGetBinaryObject


def test_ftpCreateInstance():
    mock_ftp_instance = Mock()
    ftplib.FTP = MagicMock(return_value=mock_ftp_instance)
    actual = ftpCreateInstance('host', 'user', 'passwd')
    assert mock_ftp_instance == actual

def test_ftpListFiles():
    mock_ftp_object = Mock()
    mock_list = Mock()

    ftplib.FTP = MagicMock(return_value=mock_ftp_object)
    mock_ftp_object.nlst = MagicMock(return_value=mock_list)
    actual = ftpListFiles(mock_ftp_object, 'directory')

    assert mock_list == actual


def test_ftpGetBinaryObject():
    mock_ftp_object = Mock()
    expected = io.BytesIO()

    ftplib.FTP = MagicMock(return_value=mock_ftp_object)
    actual = ftpGetBinaryObject(mock_ftp_object, 'file')

    assert expected.read() == actual.read()


