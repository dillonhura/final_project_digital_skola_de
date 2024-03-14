-- Create the ETL aggregated database
CREATE DATABASE etl_aggregated;

-- Connect to the ETL aggregated database
\c etl_aggregated;

-- Dimension table: Province
CREATE TABLE Province (
    province_id SERIAL PRIMARY KEY,
    province_name VARCHAR(255) NOT NULL
);

-- Dimension table: District
CREATE TABLE District (
    district_id SERIAL PRIMARY KEY,
    province_id INT NOT NULL,
    district_name VARCHAR(255) NOT NULL,
    FOREIGN KEY (province_id) REFERENCES Province(province_id)
);

-- Dimension table: Case
CREATE TABLE Case (
    case_id SERIAL PRIMARY KEY,
    status_name VARCHAR(255) NOT NULL,
    status_detail VARCHAR(255)
);

-- Fact table: Province Daily
CREATE TABLE ProvinceDaily (
    id SERIAL PRIMARY KEY,
    province_id INT NOT NULL,
    case_id INT NOT NULL,
    date DATE NOT NULL,
    total INT NOT NULL
);

-- Fact table: Province Monthly
CREATE TABLE ProvinceMonthly (
    id SERIAL PRIMARY KEY,
    province_id INT NOT NULL,
    case_id INT NOT NULL,
    month VARCHAR(255) NOT NULL,
    total INT NOT NULL
);

-- Fact table: Province Yearly
CREATE TABLE ProvinceYearly (
    id SERIAL PRIMARY KEY,
    province_id INT NOT NULL,
    case_id INT NOT NULL,
    year INT NOT NULL,
    total INT NOT NULL
);

-- Fact table: District Monthly
CREATE TABLE DistrictMonthly (
    id SERIAL PRIMARY KEY,
    district_id INT NOT NULL,
    case_id INT NOT NULL,
    month VARCHAR(255) NOT NULL,
    total INT NOT NULL
);

-- Fact table: District Yearly
CREATE TABLE DistrictYearly (
    id SERIAL PRIMARY KEY,
    district_id INT NOT NULL,
    case_id INT NOT NULL,
    year INT NOT NULL,
    total INT NOT NULL
);

