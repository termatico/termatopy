from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import Mock
import ftplib


def test_ftpCreateInstance():
    mock_ftp_instance = Mock()
    ftplib.FTP = MagicMock(return_value=mock_ftp_instance)
    actual = ftpCreateInstance('host', 'user', 'passwd')
    assert mock_ftp_instance == actual
