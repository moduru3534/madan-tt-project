CREATE OR REPLACE PROCEDURE <% database %>.trading_use_case.sp_load_bronze_to_employee_attendence_snowpark()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import when_not_matched

def main(session: Session) -> str:
    target_table = session.table(
        "<% database %>.trading_use_case.employee_attendance_silver"
    )

    source_table = (
        session.table(
            "<% database %>.trading_use_case.employee_information_bronze"
        )
        .select("day_id", "emp_id")
        .distinct()
    )

    merge_condition = (
        (target_table["day_id"] == source_table["day_id"])
        & (target_table["emp_id"] == source_table["emp_id"])
    )

    target_table.merge(
        source_table,
        merge_condition,
        [
            when_not_matched().insert(
                {
                    "day_id": source_table["day_id"],
                    "emp_id": source_table["emp_id"],
                }
            )
        ],
    )

    return "employee attendance silver loaded successfully"
$$;
