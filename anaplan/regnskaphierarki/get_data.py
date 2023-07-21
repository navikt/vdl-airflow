import csv
from io import StringIO
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def get_artskonti_data():
    with SnowflakeHook().get_cursor() as cursor:
        cursor.execute(
            """
            select *
            from reporting.microstrategy.dim_artskonti
            where
                er_budsjetterbar = 1 and
                artskonti_segment_kode_niva_1 is not null
            """
        )
        column_names = map(lambda x: x[0], cursor.description)
        result = cursor.fetchall()
        print(f"Number of rows: {len(result)}")
        f = StringIO(newline="")
        writer = csv.writer(f)
        writer.writerow(column_names)
        writer.writerows(result)
        return f.getvalue()
