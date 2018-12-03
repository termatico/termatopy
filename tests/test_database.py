import pandas as pd


def test_convert_column_type():
    import ast
    from psycopg2.extras import Json
    from termatopy.database import convertColumnType

    column_types = dict()
    column_types["col1"] = "text"
    column_types["col2"] = "int"
    column_types["col3"] = "json"
    column_types["col4"] = "json"

    column_values = dict()
    column_values["col1"] = ['a', 'b', 'c', 'd']
    column_values["col2"] = [1, 2, 3, 4]
    column_values["col3"] = ['{\'k1\':\'v1\'}', '{\'k2\':\'v2\'}', '{\'k3\':\'v3\'}', '{\'k4\':\'v4\'}']
    column_values["col4"] = [{'k1': 'v1'}, {'k2': 'v2'}, {'k3': 'v3'}, {'k4': 'v4'}]

    df = pd.DataFrame.from_dict(column_values)

    expected1 = column_values.get("col1")[0]
    actual1 = convertColumnType("col1", df.iloc[0], column_types)
    assert expected1 == actual1

    expected2 = column_values.get("col2")[0]
    actual2 = convertColumnType("col2", df.iloc[0], column_types)
    assert expected2 == actual2

    expected3 = Json(ast.literal_eval(column_values.get("col3")[0]))
    actual3 = convertColumnType("col3", df.iloc[0], column_types)
    assert expected3.adapted == actual3.adapted

    expected4 = Json(column_values.get("col4")[0])
    actual4 = convertColumnType("col4", df.iloc[0], column_types)
    assert expected4.adapted == actual4.adapted


def test_insert_to_postgres_sql_plain():
    from termatopy.database import insertToPostgresSqlPlain

    relation = "public"
    table_name = "table_name"
    column_list = ["col1", "col2", "col3", "col4"]
    value_list = ["val1", "val2", "val3", "val4"]

    expected_sql_insert = 'INSERT INTO public.table_name ' \
                          '(col1, col2, col3, col4) ' \
                          'VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING'
    actual_sql_insert = insertToPostgresSqlPlain(relation, table_name, column_list, value_list, [])

    assert expected_sql_insert == actual_sql_insert

    unique_key_list = ["col1", "col2"]

    expected_sql_upsert = 'INSERT INTO public.table_name ' \
                          '(col1, col2, col3, col4) ' \
                          'VALUES (%s, %s, %s, %s) ' \
                          'ON CONFLICT (col1, col2) DO UPDATE ' \
                          'SET (col3, col4) = (EXCLUDED.col3, EXCLUDED.col4)'
    actual_sql_upsert = insertToPostgresSqlPlain(relation, table_name, column_list, value_list, unique_key_list)

    assert expected_sql_upsert == actual_sql_upsert
