-- Insert into Province table
INSERT INTO province (province_name) VALUES (%s);

-- Insert into District table
INSERT INTO district (province_id, district_name) VALUES (%s, %s);

-- Insert into Case table
INSERT INTO case_table (
    status_name, status_detail, closecontact, closecontact_dikarantina, 
    closecontact_discarded, closecontact_meninggal, confirmation_meninggal, 
    confirmation_sembuh, probable, probable_diisolasi, probable_discarded, 
    probable_meninggal, suspect, suspect_diisolasi, suspect_discarded, 
    suspect_meninggal, district_id, tanggal
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
);