-- Data Cleaning

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardise the Data
-- 3. Null Values/ Blank Values
-- 4. Remove columns and rows that are unecessary 


CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;



-- 1. Remove Duplicates

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions ) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions ) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions ) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;



-- 2. Standardise the Data

SELECT company, (TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company); 


SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;


SELECT DISTINCT industry
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%';


SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET COUNTRY  = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States';

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2
WHERE STR_TO_DATE(`date`, '%m/%d/%Y') IS NULL
AND `date` IS NOT NULL;

SELECT `date`
FROM layoffs_staging2
WHERE `date` LIKE '%/%/%';

UPDATE layoffs_staging2 
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y') 
WHERE `date` LIKE '%/%';

SELECT `date` 
FROM layoffs_staging2 ;

SELECT *
FROM layoffs_staging2;


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT `date`
FROM layoffs_staging2
WHERE `date` IS NULL
   OR `date` = 'NULL'
   OR STR_TO_DATE(`date`, '%Y-%m-%d') IS NULL
AND (`date` LIKE '%/%' OR `date` LIKE '%-%');
   
   
UPDATE layoffs_staging2
SET `date` = NULL
WHERE `date` = 'NULL';


UPDATE layoffs_staging2
SET `date` = NULL
WHERE STR_TO_DATE(`date`, '%Y-%m-%d') IS NULL
AND (`date` LIKE '%/%' OR `date` LIKE '%-%');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;







-- 3. Null Values/ Blank Values


SELECT *
FROM layoffs_staging2
WHERE 
    (total_laid_off = '' 
    OR total_laid_off = 'NULL' 
    OR total_laid_off = 0 
    OR total_laid_off = -1 
    OR total_laid_off IS NULL)
AND
    (percentage_laid_off = '' 
    OR percentage_laid_off = 'NULL' 
    OR percentage_laid_off = 0 
    OR percentage_laid_off = -1 
    OR percentage_laid_off IS NULL);

UPDATE layoffs_staging2
set industry = NULL
WHERE industry = '';


SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL or t1.industry = ''
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
set t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2;

-- 4. Remove columns and rows that are unecessary 

SELECT *
FROM layoffs_staging2
WHERE 
    (total_laid_off = '' 
    OR total_laid_off = 'NULL' 
    OR total_laid_off = 0 
    OR total_laid_off = -1 
    OR total_laid_off IS NULL)
AND
    (percentage_laid_off = '' 
    OR percentage_laid_off = 'NULL' 
    OR percentage_laid_off = 0 
    OR percentage_laid_off = -1 
    OR percentage_laid_off IS NULL);


DELETE 
FROM layoffs_staging2
WHERE 
    (total_laid_off = '' 
    OR total_laid_off = 'NULL' 
    OR total_laid_off = 0 
    OR total_laid_off = -1 
    OR total_laid_off IS NULL)
AND
    (percentage_laid_off = '' 
    OR percentage_laid_off = 'NULL' 
    OR percentage_laid_off = 0 
    OR percentage_laid_off = -1 
    OR percentage_laid_off IS NULL);


SELECT *
FROM layoffs_staging2;


ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


















