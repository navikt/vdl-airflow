from io import StringIO
import csv

def get_data(query:str, cursor):
    cursor.execute(query)
    column_names = map(lambda x: x[0], cursor.description)
    result = cursor.fetchall()
    print(f"Number of rows: {len(result)}")
    f = StringIO(newline="")
    writer = csv.writer(f)
    writer.writerow(column_names)
    writer.writerows(result)
    return f.getvalue()