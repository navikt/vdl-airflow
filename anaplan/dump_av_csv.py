
from get_data import get_data, transform_to_csv
import snowflake.connector
import os

snowflake_creds = {
        "user": os.environ["DBT_USR"],
        "authenticator": 'externalbrowser',
        "account": "wx23413.europe-west4.gcp",
        "role": "reporting_microstrategy",
        "warehouse": "reporting_microstrategy",
    }

     
with snowflake.connector.connect(**snowflake_creds).cursor() as cursor:
    query="""
    select *
    from reporting.microstrategy.dim_artskonti
    where
        length(artskonti_segment_kode) = 12
    """
    result, column_names = get_data(query, cursor)
    
    csv_file = transform_to_csv(data=result, column_names=column_names)

    with open(file="dump.csv", mode="w", encoding="utf-8", newline="") as f:
        f.write(csv_file.decode(encoding="utf-8"))