INSERT INTO DistrictMonthly (district_id, case_id, month, total)
SELECT 
    d.district_id,
    c.case_id,
    TO_CHAR(current_date, 'YYYY-MM'),  -- assuming you want to aggregate by month
    SUM(c.total) AS total
FROM 
    etl_staging.case_table c
INNER JOIN 
    etl_staging.district d ON c.district_id = d.district_id
GROUP BY 
    d.district_id, c.case_id, TO_CHAR(current_date, 'YYYY-MM');
