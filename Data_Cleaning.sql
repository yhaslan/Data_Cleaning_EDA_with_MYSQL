-- DATA CLEANING

-- WARNING !
-- Table Data Import Wizard failed to import all 2361 rows. Each time it got stuck after 564 rows. Even after I clean the 564th rows and 20 rows around.
-- It goes to next 20 and still stop at 564. 

-- To fix that I manually had to load the data with the following statement.

-- However, 2 errors to fix:
-- First, ERROR 3948: Loading local data is disabled. To fix this, needed to run `mysql> set global local_infile=true;` on Terminal. See README.md
-- Second, ERROR 2068 (HY000): LOAD DATA LOCAL INFILE file request rejected due to restrictions on access.` To fix this:
-- Edit the connection, on the Connection tab, go to the 'Advanced' sub-tab, and in the 'Others:' box add the line 'OPT_LOCAL_INFILE=1'. 

DROP TABLE IF EXISTS layoffs;

CREATE TABLE layoffs(
company text,
location text,
industry text,
total_laid_off INT,
percentage_laid_off text,
layoff_date text,
stage text,
country text,
funds_raised_millions INT
);


LOAD DATA LOCAL INFILE '/Users/yagmuraslan/Desktop/DataAnalyst-Portfolio/MYSQL/layoffs.csv'
INTO TABLE layoffs
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SELECT *
FROM layoffs;

SELECT COUNT(*) 
FROM layoffs;

-- DATA CLEANING STEPS

# 1. Remove Duplicates
# 2. Standardize the Data
# 3. NULL or Blank values
# 4. Remove any Columns

# Note that 4th one is espcecially risky but all of them modifies the raw dataset table
# So better to work with a copy of it to keep track.

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;


-- REMOVE DUPLICATES

# One idea s to use ROW_NUMBER() function, we can start by partitioning over a few columns to see if we get any hit, if so explore further
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, layoff_date) AS row_num
FROM layoffs_staging;
# most of them seems equal to 1 so seems to be unique

# Let's check it with a CTE
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, layoff_date) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1 ;
# Oupsie it returned 7 rows with row_num 2.

# To make sure, let's check it for all colums now:
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, layoff_date, stage, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1 ;
# ok this time 5 rows are returned, we have 5 duplicates

# We need to idetify these exact rows as we don't wanna delete both of them. In MySQL, it is a bit trickier to remove things compared to SQLServer or PostgreSQL
# Microsoft SQL Server alows identifying these row numbers in the CTE but not in MySQL

# Strategy: Create a new table with all the columns, including row_num column we created with the CTE

CREATE TABLE layoffs_staging2(
company text,
location text,
industry text,
total_laid_off INT,
percentage_laid_off text,
layoff_date text,
stage text,
country text,
funds_raised_millions INT,
row_num INT
);

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,layoff_date, stage, country, funds_raised_millions
			) AS row_num
FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2;

# Now we can delete based on the value on column row_num
DELETE
FROM layoffs_staging2
WHERE row_num >1 ;

# Error 1175: You are using safe update mode
SET SQL_SAFE_UPDATES = 0;
DELETE
FROM layoffs_staging2
WHERE row_num >1 ;

# it would have been so much easier if we had a unique_id column



-- STANDARDIZE THE DATA

SELECT DISTINcT company
FROM layoffs_staging2;

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2 #you may need to set preferences to safe update disabled => Preferences - SQL editor - box in the bottom
#you may potentially need to restart mysql
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1; # by the first column which is industry
# as you see some standardization issues (crypp, crypto currency, cryptocurrency; there is also a blank one)

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'; 

UPDATE layoffs_staging2 
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';  #updated 3 rows

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1; #fine

# now location
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1; 

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1; 

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'; 

UPDATE layoffs_staging2 
SET country = 'United States'
WHERE country LIKE 'United States%';

# if you wanna do a timeseries you need to change date column to date format
SELECT layoff_date,
STR_TO_DATE(layoff_date, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2 
SET layoff_date = STR_TO_DATE(layoff_date, '%m/%d/%Y');

SELECT layoff_date
FROM layoffs_staging2;

# you think it worked but refresh the schemas, check the table column and you still see "layoff_date text"
# to change it efectively you need ALTER, but do never use that on raw table

ALTER TABLE layoffs_staging2
MODIFY COLUMN layoff_date DATE;

SELECT layoff_date
FROM layoffs_staging2;

# now when you refresh you see the column type modified


-- NULL AND BLANK VALUES

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL; # (= NULL would return nothing)

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';
# we are gonna try to populate these with a join

SELECT * 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry is NULL or t1.industry = '')
AND t2.industry is NOT NULL ;
# I initially tried with t1.industry != t2.industry but it was not the way to go, printed out way more)

# lt's set all blanks to null first or it did not work:
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry is NULL
AND t2.industry is NOT NULL ;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = ''; # just one remained because do not appear on the table again

SELECT*
FROM layoffs_staging2
WHERE company = "Bally's Interactive"; # exactly there is only one of it

# things like total_laid_off or percentage_laid_off are not possible here to populate w/o having company totals in the first place 
# some of them has NULL both for total_laid_off and percentage_laid_off, maybe there were no laoyff, wdk

# don't think we need that info probably, can delete
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;


-- REMOVE ANY COLUMNS
# finally we no longer need column row num
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;