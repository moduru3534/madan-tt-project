CREATE OR REPLACE PROCEDURE <% database %>.TRADING_USE_CASE.SP_LOAD_BRONZE_TO_DEPARTMENTS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN

    MERGE INTO <% database %>.trading_use_case.departments_silver tgt
    USING
    (
        SELECT DISTINCT
            dept_id,
            dept_name,
            dept_code
        FROM <% database %>.trading_use_case.employee_information_bronze
    ) src
    ON tgt.dept_id = src.dept_id

    WHEN MATCHED THEN UPDATE SET
        tgt.dept_name = src.dept_name,
        tgt.dept_code = src.dept_code

    WHEN NOT MATCHED THEN INSERT
    (
        dept_id,
        dept_name,
        dept_code
    )
    VALUES
    (
        src.dept_id,
        src.dept_name,
        src.dept_code
    );

    RETURN ''departments silver loaded successfully'';

END;
';
