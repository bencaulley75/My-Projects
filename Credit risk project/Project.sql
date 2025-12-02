create database credit_risk_db;
use credit_risk_db;
CREATE TABLE credit_risk (
    person_age INT NULL,
    person_income DECIMAL(12 , 2 ) NULL,
    person_home_ownership VARCHAR(50) NULL,
    person_emp_length INT NULL,
    loan_intent VARCHAR(50) NULL,
    loan_grade CHAR(1) NULL,
    loan_amnt DECIMAL(12 , 2 ) NULL,
    loan_int_rate DECIMAL(5 , 2 ) NULL,
    loan_status VARCHAR(20) NULL,
    loan_percent_income DECIMAL(5 , 2 ) NULL,
    cb_person_default_on_file VARCHAR(5) NULL,
    cb_person_cred_hist_length INT NULL
);
SELECT 
    *
FROM
    credit_risk;
SHOW VARIABLES LIKE 'sql_mode';
SET GLOBAL sql_mode='';
-- Cleaning the dataset
-- Check for null values
SELECT 
    *
FROM
    credit_risk
WHERE
    person_income IS NULL
        OR loan_amnt IS NULL
        OR loan_int_rate IS NULL
        OR loan_percent_income IS NULL
        OR cb_person_cred_hist_length IS NULL;
        
-- Remove duplicates 
