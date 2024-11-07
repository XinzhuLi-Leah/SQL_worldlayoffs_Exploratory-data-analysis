SELECT * FROM world_layoffs.layoffs;

-- 1.remove duplicates;
-- 2.standardize the data
-- 3.null  values or blank values
-- 4.remove any columns

-- keep the raw data,creat the copied data   保留原始数据 总是创建一个复制的数据集
Create table layoffs_staging 
like layoffs;

Insert layoffs_staging
select * from layoffs

-- 1.
--  we can easily find the duplicateted rows through row_number。
-- 通过row_number赋予每一行编码，我们就能找到重复的行，如果说有两行一模一样的行，那么ID就是有2，有三行一样的，那么ID就会有2，3
with duplicate_cte as 
(
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
select *
from duplicate_cte
where row_num >1

SELECT * FROM layoffs_staging
where company = 'casper'

with duplicate_cte as 
(
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
delete *
from duplicate_cte
where row_num >1                   -- this is wrong 


-- how to delete the duplicates 删除重复的数据行 上述的cte只能是查询出来，没办法说直接写上delete语句
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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2


-- successfully put the row_num in the table
insert into layoffs_staging2
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
 
SET SQL_SAFE_UPDATES = 0;

DELETE FROM layoffs_staging2
WHERE row_num > 1;

select *  FROM layoffs_staging2
WHERE row_num > 1;


-- 2.
-- standardizing data

select company, trim(company)。-- trim： take of the blank space 用于移除字符串两端的空白字符（包括空格、换行符等）
from layoffs_staging2

update layoffs_staging2
set company= trim(company)

select distinct(industry)
from layoffs_staging2
order by 1

select *
from layoffs_staging2
where industry like 'crypto'

update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%'      -- 行业名字统一起来

select distinct country
from layoffs_staging2
order by 1

select *
from layoffs_staging2
where country like '%states.%'   -- 国家名字统一起来  

update layoffs_staging2
set country = 'United States' 
where country like '%states.%'


-- change the column type

-- 	•	STR_TO_DATE()：Primarily used to convert text into a date format.
-- 	•	DATE_FORMAT()：Format a date/time type as a string for display, according to the specified format.

UPDATE layoffs_staging2
SET date = NULL
WHERE date = 'NULL'

UPDATE layoffs_staging2
SET DATE = str_to_date(date,'%m/%d/%Y') 

ALTER TABLE layoffs_staging2
MODIFY COLUMN date DATE;


-- 3. 
-- dealign with the NULL
-- NULL may be incorrectly stored as the string ‘NULL’ 

UPDATE layoffs_staging2
SET total_laid_off = NULL
WHERE total_laid_off = 'NULL';

UPDATE layoffs_staging2
SET percentage_laid_off = NULL
WHERE percentage_laid_off = 'NULL'; 

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = 'NULL'; 

UPDATE layoffs_staging2
SET funds_raised_millions = NULL
WHERE funds_raised_millions = 'NULL'; 


SELECT * FROM world_layoffs.layoffs_staging2 
where industry is null     
--  results: 4 companies

-- for example: we are going to populate the data by using join

SELECT * FROM world_layoffs.layoffs_staging2 
where company like '%Bally%'


select t1.industry,t2.industry
from layoffs_staging2  as t1
left join layoffs_staging2  as t2
  on t1.company=t2.company
where t1.industry is null and t2.industry is not null

update layoffs_staging2  as t1
left join layoffs_staging2  as t2
  on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null and t2.industry is not null
-- The UPDATE statement can indeed be used in conjunction with JOIN,
-- which is a common operation in SQL. The main purpose of using a join to update data is to modify records in the target table based on data from another table

-- 4.
-- so actually these data are useless (in my opinion)
select * 
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null   

delete 
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null   


alter table layoffs_staging2
drop column row_num

select * 
from layoffs_staging2
