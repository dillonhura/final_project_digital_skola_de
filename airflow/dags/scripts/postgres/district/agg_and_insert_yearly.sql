INSERT INTO DistrictYearly (district_id, case_id, year, total)
SELECT 
    d.district_id,
    c.case_id,
    EXTRACT(YEAR FROM current_date),  -- assuming you want to aggregate by year
    SUM(c.total) AS total
FROM 
    etl_staging.case_table c
INNER JOIN 
    etl_staging.district d ON c.district_id = d.district_id
GROUP BY 
    d.district_id, c.case_id, EXTRACT(YEAR FROM current_date);
