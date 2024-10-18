-- Data Cleaning


SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank values
-- 4. Remove unneccessary columns or row



CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`) as row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company='Casper';

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;


SELECT *
FROM layoffs_staging2;


-- Standardizing Data

SELECT company,TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2 WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto' WHERE industry LIKE 'Crypto%';

SELECT  country
FROM layoffs_staging2 where country like 'United States%'
order by 1;

SELECT  DISTINCT country, trim(trailing '.' from country)
FROM layoffs_staging2 
order by 1;



SELECT `date`,
str_to_date(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

update layoffs_staging2 
set `date`=str_to_date(`date`,'%m/%d/%Y');


alter table layoffs_staging2
modify column `date` date;

select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

update layoffs_staging2
set industry=null
where industry='';

select * from layoffs_staging2 where industry is null or industry = '';

select * from layoffs_staging2 where company like 'Bally%';

select t1.industry,t2.industry
from layoffs_staging2 t1 
join layoffs_staging2 t2 
on t1.company=t2.company
and t1.location = t2.location
where (t1.industry is null or t1.industry='')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null
and t2.industry is not null;

select * from layoffs_staging2;

select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

delete
from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

select * from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;
