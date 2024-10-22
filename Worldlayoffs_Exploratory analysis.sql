-- Exploratory data analysis

-- find the date range  '2020-03-11'-'2023-03-06'
select min(date),max(date) from layoffs_staging2

-- put the country in the code to help the interactive visulizaiton in Tableau
-- find the data according to the company   公司
select country,company,sum(total_laid_off) as total
from layoffs_staging2
group by country,company
order by total desc
limit 20;


-- find the data according to the industry 行业
select country,industry,sum(total_laid_off) as total
from layoffs_staging2
group by country,industry
order by total desc


-- find the data according to the country  国家
select country,sum(total_laid_off) as total
from layoffs_staging2
group by country
order by total desc


-- find the data according to the stage  阶段
select stage,sum(total_laid_off) as total
from layoffs_staging2
where stage != 'null' and stage != 'unknown'
group by stage
order by total desc

-- total layoffs in the past 4 years
select year(date) as years,sum(total_laid_off) as total
from layoffs_staging2
where year(date) is not null
group by year(date)
order by year(date) desc

 

-- aggregated numbers by year + month
select substring(date,1,7) as month,sum(total_laid_off) as total
from layoffs_staging2
where substring(date,1,7) is not null
group by month
order by month asc 

-- rolling layoffs
-- using subquery + sum window function
select *,sum(total_layoffs) over(order by month) as rolling_layoffs
from
(
select substring(date,1,7) as month,sum(total_laid_off) as total_layoffs
from layoffs_staging2
where substring(date,1,7) is not null
group by month
order by month asc 
) as tmp1

-- rolling layoffs
-- using CTE + sum window function
with tmp1 as 
(
select substring(date,1,7) as month,sum(total_laid_off) as total_layoffs
from layoffs_staging2
where substring(date,1,7) is not null
group by month
order by month asc
)
select *,sum(total_layoffs) over(order by month) as rolling_layoffs
from tmp1


-- ranking by using window function
With tmp1 as
(
select year(date) as years,company,sum(total_laid_off) as total_layoffs
from layoffs_staging2
where year(date) is not null
group by years,company
)
select *, dense_rank()over(partition by years order by total_layoffs desc) as ranking
from tmp1
where years is not null
order by ranking 


-- pick out the top 5 companies every year
With tmp1 as
(
select year(date) as years,company,sum(total_laid_off) as total_layoffs
from layoffs_staging2
where year(date) is not null
group by years,company
),
tmp2 as
(
select *, dense_rank()over(partition by years order by total_layoffs desc) as ranking
from tmp1
where years is not null
order by ranking 
)
select * from tmp2
where ranking <= 5




