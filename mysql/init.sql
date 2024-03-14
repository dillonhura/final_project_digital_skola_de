-- Create the ETL staging database
CREATE DATABASE etl_staging;

-- Use the ETL staging database
USE etl_staging;

-- Create the necessary tables for the ETL process

-- Province table
CREATE TABLE province (
    province_id INT AUTO_INCREMENT PRIMARY KEY,
    province_name VARCHAR(255) NOT NULL
);

-- District table
CREATE TABLE district (
    district_id INT AUTO_INCREMENT PRIMARY KEY,
    province_id INT NOT NULL,
    district_name VARCHAR(255) NOT NULL,
    FOREIGN KEY (province_id) REFERENCES province(province_id)
);

-- Case table
CREATE TABLE case_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    status_name VARCHAR(255) NOT NULL,
    status_detail INT NOT NULL,
    closecontact INT NOT NULL,
    closecontact_dikarantina INT NOT NULL,
    closecontact_discarded INT NOT NULL,
    closecontact_meninggal INT NOT NULL,
    confirmation_meninggal INT NOT NULL,
    confirmation_sembuh INT NOT NULL,
    probable INT NOT NULL,
    probable_diisolasi INT NOT NULL,
    probable_discarded INT NOT NULL,
    probable_meninggal INT NOT NULL,
    suspect INT NOT NULL,
    suspect_diisolasi INT NOT NULL,
    suspect_discarded INT NOT NULL,
    suspect_meninggal INT NOT NULL,
    district_id INT NOT NULL,
    tanggal DATE NOT NULL,
    FOREIGN KEY (district_id) REFERENCES district(district_id)
);
