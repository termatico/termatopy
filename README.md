# Termatopy
[![CircleCI](https://circleci.com/gh/termatico/termatopy.svg?style=svg&circle-token=76c7dfd831dacc25de69aa409d585d701b2fc5de)](https://circleci.com/gh/termatico/termatopy)
[![codecov](https://codecov.io/gh/termatico/termatopy/branch/master/graph/badge.svg)](https://codecov.io/gh/termatico/termatopy)

A Python package for working with the Termatico environment.

`pip install git+https://github.com/termatico/termatopy.git`

### Source 🐧
To avoid pushing any keys used in the various services Termatopy provides an interface for (such as AWS), please use the `source()` function to load in a file containing the keys you need.

To set this up you need to have a file called `.profile` saved in your home directoty. Mac OS is the only operating system supported at the moment. This file should be a valid JSON file with the following structure:

```
{
  "name_of_service" : {
     "key": "value",
     "secret" : "value"
     ...
  }
}
```

You can then access these keys using Python dictionaries such as:

```
env = source()
env['name_of_service']['key']
env['name_of_service']['secret']
```
