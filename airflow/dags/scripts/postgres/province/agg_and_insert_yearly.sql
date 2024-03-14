INSERT INTO ProvinceYearly (province_id, case_id, year, total)
SELECT 
    p.province_id,
    c.case_id,
    EXTRACT(YEAR FROM current_date),  -- assuming you want to aggregate by year
    SUM(c.total) AS total
FROM 
    etl_staging.case_table c
INNER JOIN 
    etl_staging.district d ON c.district_id = d.district_id
INNER JOIN 
    etl_staging.province p ON d.province_id = p.province_id
GROUP BY 
    p.province_id, c.case_id, EXTRACT(YEAR FROM current_date);