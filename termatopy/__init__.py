import warnings
warnings.filterwarnings("ignore")
from .aws import fetchS3
from .aws import invokeLambda
from .aws import putS3
from .aws import listFiles
from .aws import extractBucketName
from .aws import checkFileType
from .aws import s3ObjectSummaryMetaData
from .aws import s3ObjectSummaryMetaDataList
from .database import queryPostgres
from .database import runQueryPostgres
from .database import queryMongo
from .database import queryAthena
from .database import getExecutionStatus
from .database import createFieldReplacement
from .database import insertToPostgres
from .outputs import output
from .outputs import Logger
from .inputs import profile
from .inputs import source
from .termatico import runTemplate
from .ftp import ftpCreateInstance
from .ftp import ftpListFiles
from .ftp import ftpGetBinaryObject
from .ftp import zipReadFile
from .ftp import ftpReadDelimitedText


import numpy
from psycopg2.extensions import register_adapter, AsIs


def adapt_numpy_int64(numpy_int64):
    """ Adapting numpy.int64 type to SQL-conform int type using psycopg extension, see [1]_ for more info.
    References
    ----------
    .. [1] http://initd.org/psycopg/docs/advanced.html#adapting-new-python-types-to-sql-syntax
    """
    return AsIs(numpy_int64)


register_adapter(numpy.int64, adapt_numpy_int64)

import numpy
from psycopg2.extensions import register_adapter, AsIs


def adapt_numpy_bool(numpy_bool):
    """ Adapting numpy.bool_ type to SQL-conform int type using psycopg extension, see [1]_ for more info.
    References
    ----------
    .. [1] http://initd.org/psycopg/docs/advanced.html#adapting-new-python-types-to-sql-syntax
    """
    return AsIs(numpy_bool)


register_adapter(numpy.bool_, adapt_numpy_bool)
