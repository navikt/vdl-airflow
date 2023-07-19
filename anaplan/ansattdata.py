import oracledb
import csv
from io import StringIO


def get_data():
    creds = {"user": "kafka", "password": "example", "dsn": "localhost:1521/XEPDB1"}
    batch_size = 1

    with oracledb.connect(**creds) as con:
        with con.cursor() as cur:
            cur.execute("select * from test_data")
            column_names = map(lambda x: x[0], cur.description)
            with open("ansattdata.csv", "w", newline="") as f:
                f = StringIO(newline="")
                writer = csv.writer(f)
                writer.writerow(column_names)
                while result := cur.fetchmany(batch_size):
                    writer.writerows(result)
                f.seek(0)
                return f


# print(f.getvalue())
