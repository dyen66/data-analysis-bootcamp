SELECT * 
FROM layoffs;

-- Data Cleaning
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Missing Values
-- 4. Remove cols

CREATE TABLE layoff_staging
LIKE layoffs;

INSERT layoff_staging
SELECT *
FROM layoffs;

SELECT * 
FROM layoff_staging;

WITH duplicate_cte AS 
(
SELECT*,
ROW_NUMBER() OVER(PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoff_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1; -- no duplicates

-- Standardizing data

SELECT company, TRIM(company)
FROM layoff_staging;

UPDATE layoff_staging
SET company = TRIM(company);

SELECT DISTINCT country
FROM layoff_staging
ORDER BY 1;

SELECT DISTINCT industry
FROM layoff_staging
ORDER BY 1;

SELECT *
FROM layoff_staging
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoff_staging
ORDER BY 1;

UPDATE layoff_staging
SET industry = 'Travel'
WHERE company = 'Airbnb';

SELECT *
FROM layoff_staging;

UPDATE layoff_staging
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoff_staging -- never do this on the raw dataset
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoff_staging
WHERE industry IS NULL OR industry ='';

SELECT *
FROM layoff_staging
WHERE company = 'Airbnb';

SELECT *
FROM layoff_staging
WHERE company = "Bally's Interactive";

SELECT t1.company, t1.industry, t2.industry
FROM layoff_staging t1
JOIN layoff_staging t2 ON t1.company = t2.company  
WHERE (t1.industry IS NULL OR t1.industry = '') AND t2.industry IS NOT NULL;

SELECT *
FROM layoff_staging;

DELETE FROM layoff_staging 
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;


SELECT total_laid_off, percentage_laid_off
FROM layoff_staging 
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;


