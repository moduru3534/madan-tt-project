CREATE OR REPLACE PROCEDURE <% database %>.TRADING_USE_CASE.SP_LOAD_SILVER_TO_GOLD_SNOWPARK()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN

    MERGE INTO <% database %>.trading_use_case.attendance_by_department tgt
    USING
    (
        SELECT
            att.day_id,
            dept.dept_id,
            dept.dept_code,
            COUNT(emp.emp_id) AS employee_count
        FROM <% database %>.trading_use_case.employees_silver emp
        JOIN <% database %>.trading_use_case.departments_silver dept
            ON emp.dept_id = dept.dept_id
        JOIN <% database %>.trading_use_case.employee_attendance_silver att
            ON att.emp_id = emp.emp_id
        GROUP BY att.day_id, dept.dept_id, dept.dept_code
    ) src
    ON tgt.day_id = src.day_id
        AND tgt.dept_id = src.dept_id

    WHEN MATCHED THEN UPDATE SET
        tgt.dept_code = src.dept_code,
        tgt.employee_count = src.employee_count

    WHEN NOT MATCHED THEN INSERT
    (
        day_id,
        dept_id,
        dept_code,
        employee_count
    )
    VALUES
    (
        src.day_id,
        src.dept_id,
        src.dept_code,
        src.employee_count
    );

    RETURN ''attendance_by_department gold loaded successfully'';

END;
';
