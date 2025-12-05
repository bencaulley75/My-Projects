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
-- DELETE database credit_risk_db;
-- SELECT * FROM  credit_risk;

LOAD DATA LOCAL INFILE '/Users/bennettcaulley/Desktop/Credit risk project/credit_risk_dataset.csv'
INTO TABLE credit_risk
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    person_age,
    person_income,
    person_home_ownership,
    person_emp_length,
    loan_intent,
    loan_grade,
    loan_amnt,
    loan_int_rate,
    loan_status,
    loan_percent_income,
    cb_person_default_on_file,
    cb_person_cred_hist_length
)
SET
    person_age                 = NULLIF(person_age, ''),
    person_income              = NULLIF(person_income, ''),
    person_home_ownership      = NULLIF(person_home_ownership, ''),
    person_emp_length          = NULLIF(person_emp_length, ''),
    loan_intent                = NULLIF(loan_intent, ''),
    loan_grade                 = NULLIF(loan_grade, ''),
    loan_amnt                  = NULLIF(loan_amnt, ''),
    loan_int_rate              = NULLIF(loan_int_rate, ''),
    loan_status                = NULLIF(loan_status, ''),
    loan_percent_income        = NULLIF(loan_percent_income, ''),
    cb_person_default_on_file  = NULLIF(cb_person_default_on_file, ''),
    cb_person_cred_hist_length = NULLIF(cb_person_cred_hist_length, '');


SELECT 
    *
FROM
    credit_risk;



-- Cleaning the dataset

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

-- check the total number of null values 
SELECT 
    SUM(person_age IS NULL) AS age_nulls,
    SUM(person_income IS NULL) AS income_nulls,
    SUM(person_home_ownership IS NULL) AS home_nulls,
    SUM(person_emp_length IS NULL) AS emp_nulls,
    SUM(loan_intent IS NULL) AS intent_nulls,
    SUM(loan_grade IS NULL) AS grade_nulls,
    SUM(loan_amnt IS NULL) AS loan_nulls,
    SUM(loan_int_rate IS NULL) AS rate_nulls,
    SUM(loan_status IS NULL) AS status_nulls,
    SUM(loan_percent_income IS NULL) AS percent_income_nulls,
    SUM(cb_person_default_on_file IS NULL) AS default_nulls,
    SUM(cb_person_cred_hist_length IS NULL) AS hist_nulls
FROM
    credit_risk;

-- Replacing Nulls with Averages
UPDATE credit_risk AS t
        JOIN
    (SELECT 
        AVG(person_age) AS avg_age,
            AVG(person_income) AS avg_income,
            AVG(person_emp_length) AS avg_emp_length,
            AVG(loan_amnt) AS avg_loan_amnt,
            AVG(loan_int_rate) AS avg_loan_int_rate,
            AVG(loan_percent_income) AS avg_loan_percent_income,
            AVG(cb_person_cred_hist_length) AS avg_hist
    FROM
        credit_risk) AS sub 
SET 
    t.person_age = COALESCE(t.person_age, sub.avg_age),
    t.person_income = COALESCE(t.person_income, sub.avg_income),
    t.person_emp_length = COALESCE(t.person_emp_length, sub.avg_emp_length),
    t.loan_amnt = COALESCE(t.loan_amnt, sub.avg_loan_amnt),
    t.loan_int_rate = COALESCE(t.loan_int_rate, sub.avg_loan_int_rate),
    t.loan_percent_income = COALESCE(t.loan_percent_income,
            sub.avg_loan_percent_income),
    t.cb_person_cred_hist_length = COALESCE(t.cb_person_cred_hist_length,
            sub.avg_hist),
    t.person_home_ownership = COALESCE(t.person_home_ownership, 'Unknown'),
    t.loan_intent = COALESCE(t.loan_intent, 'Unknown'),
    t.loan_grade = COALESCE(t.loan_grade, 'Unknown'),
    t.loan_status = COALESCE(t.loan_status, 'Unknown'),
    t.cb_person_default_on_file = COALESCE(t.cb_person_default_on_file, 'Unknown')
WHERE
    t.person_age IS NULL
        OR t.person_income IS NULL
        OR t.person_emp_length IS NULL
        OR t.loan_amnt IS NULL
        OR t.loan_int_rate IS NULL
        OR t.loan_percent_income IS NULL
        OR t.cb_person_cred_hist_length IS NULL
        OR t.person_home_ownership IS NULL
        OR t.loan_intent IS NULL
        OR t.loan_grade IS NULL
        OR t.loan_status IS NULL
        OR t.cb_person_default_on_file IS NULL;


-- Checking to see if there are still null values
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

        
-- Validating Numeric Ranges 
SELECT 
    *
FROM
    credit_risk
WHERE
    person_age < 18 OR person_age > 100
        OR loan_int_rate < 0
        OR loan_int_rate > 100
        OR loan_percent_income < 0
        OR loan_percent_income > 1;






-- Adding a new column to know the outliers in terms of age
ALTER TABLE credit_risk ADD age_flag VARCHAR(10);

UPDATE credit_risk 
SET 
    age_flag = CASE
        WHEN person_age < 18 OR person_age > 100 THEN 'Outlier'
        ELSE 'Valid'
    END;

SELECT 
    *
FROM
    credit_risk;

-- Adding a new column to know the outliers in terms of person employment length
ALTER TABLE credit_risk ADD person_emp_flag VARCHAR(10);

UPDATE credit_risk 
SET 
    person_emp_flag = CASE
        WHEN
            person_emp_length < 1
                OR person_emp_length > 15
        THEN
            'Outlier'
        ELSE 'Valid'
    END;

SELECT 
    *
FROM
    credit_risk;

/* Creating a derived column for income*/
ALTER TABLE credit_risk ADD income_group VARCHAR(20);

UPDATE credit_risk 
SET 
    income_group = CASE
        WHEN person_income <= 11000 THEN 'Low'
        WHEN person_income BETWEEN 11000 AND 30000 THEN 'Medium'
        ELSE 'High'
    END;
SELECT 
    *
FROM
    credit_risk
WHERE
    income_group = 'Low';


   -- Creating a cleaned version
CREATE TABLE credit_risk_clean AS SELECT * FROM
    credit_risk
WHERE
    person_age BETWEEN 18 AND 100
        AND loan_int_rate BETWEEN 0 AND 100
        AND loan_percent_income BETWEEN 0 AND 1
        AND person_emp_length BETWEEN 1 AND 15;
   
SELECT 
    *
FROM
    credit_risk_clean;

-- ANALYSIS
/*1. Age Distribution of Applicants*/
SELECT 
    person_age, COUNT(*) AS applicant_count
FROM
    credit_risk_clean
GROUP BY person_age
ORDER BY person_age;

/*2. Loan Intent Breakdown (What are the most common purposes for loans)*/
SELECT 
    loan_intent, COUNT(*) AS loan_count
FROM
    credit_risk_clean
GROUP BY loan_intent
ORDER BY loan_count DESC;

/*3. Income vs. Loan Amount (How does applicant income compare to loan amounts requested?)*/
SELECT 
    person_income, loan_amnt
FROM
    credit_risk_clean;

/*4. Loan Grade Distribution (How are loans distributed across grades (A, B, C, etc.)?)*/
SELECT 
    loan_grade, COUNT(*) AS grade_count
FROM
    credit_risk_clean
GROUP BY loan_grade
ORDER BY loan_grade;

/* Updating table to convert numbers to text*/
ALTER TABLE credit_risk_clean ADD loan_status_text VARCHAR(20);

UPDATE credit_risk_clean 
SET 
    loan_status_text = CASE
        WHEN loan_status = '1' THEN 'Default'
        WHEN loan_status = '0' THEN 'Non-Default'
    END;

SELECT 
    *
FROM
    credit_risk_clean;

/* 5.  Default Loan Percent of Income Analysis (How risky are loans relative to applicant income?) */
SELECT 
    loan_percent_income,
    SUM(CASE
        WHEN loan_status_text = 'Default' THEN 1
        ELSE 0
    END) AS defaults,
    SUM(CASE
        WHEN loan_status_text = 'Non-Default' THEN 1
        ELSE 0
    END) AS non_defaults,
    COUNT(*) AS total_loans,
    ROUND(SUM(CASE
                WHEN loan_status_text = 'Default' THEN 1
                ELSE 0
            END) * 100.0 / COUNT(*),
            2) AS default_rate,
    ROUND(SUM(CASE
                WHEN loan_status_text = 'Non-Default' THEN 1
                ELSE 0
            END) * 100.0 / COUNT(*),
            2) AS non_default_rate
FROM
    credit_risk_clean
GROUP BY loan_percent_income
ORDER BY loan_percent_income;

/*6. Default Rate by Employment Length (Does employment length affect loan default rates?)  */
SELECT 
    person_emp_length,
    SUM(CASE
        WHEN loan_status_text = 'Default' THEN 1
        ELSE 0
    END) AS defaults,
    COUNT(*) AS total_loans,
    (SUM(CASE
        WHEN loan_status_text = 'Default' THEN 1
        ELSE 0
    END) * 100.0 / COUNT(*)) AS default_rate
FROM
    credit_risk_clean
GROUP BY person_emp_length
ORDER BY person_emp_length;

/* 7. Credit History vs. Default  (How does credit history length relate to default probability?) */
SELECT 
    cb_person_cred_hist_length,
    SUM(CASE
        WHEN loan_status_text = 'Default' THEN 1
        ELSE 0
    END) AS defaults,
    SUM(CASE
        WHEN loan_status_text = 'Non-Default' THEN 1
        ELSE 0
    END) AS non_defaults,
    COUNT(*) AS total_loans,
    ROUND(SUM(CASE
                WHEN loan_status_text = 'Default' THEN 1
                ELSE 0
            END) * 100.0 / COUNT(*),
            2) AS default_rate,
    ROUND(SUM(CASE
                WHEN loan_status_text = 'Non-Default' THEN 1
                ELSE 0
            END) * 100.0 / COUNT(*),
            2) AS non_default_rate
FROM
    credit_risk_clean
GROUP BY cb_person_cred_hist_length
ORDER BY cb_person_cred_hist_length;
SELECT 
    *
FROM
    credit_risk_clean;
    
/*8. Income vs. Home Ownership (How does applicant income vary across home ownership categories)*/
SELECT 
    person_home_ownership as 'Home Ownership',
    ROUND(AVG(person_income), 2) AS 'Average Income ($)'
FROM credit_risk_clean
GROUP BY person_home_ownership
ORDER BY AVG(person_income) DESC;

/* KPIs*/
SELECT 
    COUNT(*) AS total_loans
FROM
    credit_risk_clean;

/*2. Overall Default Rate*/
SELECT 
    (SUM(CASE
        WHEN loan_status_text = 'Default' THEN 1
        ELSE 0
    END) * 100.0 / COUNT(*)) AS default_rate
FROM
    credit_risk_clean;

/*3. Average Income */
SELECT 
    AVG(person_income) AS avg_income
FROM
    credit_risk_clean;

/*4. Average Loan Amount*/
SELECT 
    AVG(loan_amnt) AS avg_loan_amount
FROM
    credit_risk_clean;

/*5. Total Number of Applicants*/
Select count(person_age) as 'Total Number of Applicants'
from credit_risk_clean;

/*KPIs. Summary*/
SELECT 
    COUNT(*) AS 'Total Loans',
    (SUM(CASE
        WHEN loan_status_text = 'Default' THEN 1
        ELSE 0
    END) * 100.0 / COUNT(*)) AS 'Default Loan Rate',
    AVG(person_income) AS 'Average Income',
    AVG(loan_amnt) AS 'Average Loan Amount'
FROM
    credit_risk_clean;