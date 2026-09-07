CREATE OR REPLACE PROCEDURE <% database %>.trading_use_case.sp_load_bronze_to_departments_snowpark()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col,
    row_number,
    when_matched,
    when_not_matched,
)
from snowflake.snowpark.window import Window

def main(session: Session) -> str:
    target_table = session.table(
        "<% database %>.trading_use_case.departments_silver"
    )

    source_table = (
        session.table(
            "<% database %>.trading_use_case.employee_information_bronze"
        )
        .select(
            "dept_id",
            "dept_name",
            "dept_code",
        )
        .with_column(
            "row_num",
            row_number().over(
                Window.partition_by("dept_id").order_by(
                    col("dept_name"),
                    col("dept_code"),
                )
            ),
        )
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    merge_condition = (
        target_table["dept_id"] == source_table["dept_id"]
    )

    target_table.merge(
        source_table,
        merge_condition,
        [
            when_matched().update(
                {
                    "dept_name": source_table["dept_name"],
                    "dept_code": source_table["dept_code"],
                }
            ),
            when_not_matched().insert(
                {
                    "dept_id": source_table["dept_id"],
                    "dept_name": source_table["dept_name"],
                    "dept_code": source_table["dept_code"],
                }
            ),
        ],
    )

    return "departments silver loaded successfully"
$$;
