CREATE OR REPLACE PROCEDURE <% database %>.trading_use_case.sp_load_bronze_to_employees_snowpark()
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
        "<% database %>.trading_use_case.employees_silver"
    )

    source_table = (
        session.table(
            "<% database %>.trading_use_case.employee_information_bronze"
        )
        .select(
            "emp_id",
            "emp_name",
            "emp_salary",
            "dept_id",
            "emp_email",
        )
        .with_column(
            "row_num",
            row_number().over(
                Window.partition_by("emp_id").order_by(
                    col("emp_name"),
                    col("emp_salary"),
                    col("dept_id"),
                    col("emp_email"),
                )
            ),
        )
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    merge_condition = (
        target_table["emp_id"] == source_table["emp_id"]
    )

    target_table.merge(
        source_table,
        merge_condition,
        [
            when_matched().update(
                {
                    "emp_name": source_table["emp_name"],
                    "emp_salary": source_table["emp_salary"],
                    "dept_id": source_table["dept_id"],
                    "emp_email": source_table["emp_email"],
                }
            ),
            when_not_matched().insert(
                {
                    "emp_id": source_table["emp_id"],
                    "emp_name": source_table["emp_name"],
                    "emp_salary": source_table["emp_salary"],
                    "dept_id": source_table["dept_id"],
                    "emp_email": source_table["emp_email"],
                }
            ),
        ],
    )

    return "employees silver loaded successfully"
$$;
