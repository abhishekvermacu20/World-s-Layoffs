use world_layoffs;

select *
from layoffs_staging2;

-- Maximum laid off and percentage laid off
select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

-- Maximum laid off and percentage laid off where percentage laid off is 100%
select *
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc;
-- order by funds_raised_millions
select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

-- Top 10 companies laid off maximum number of employees within 3 years.
select company, sum(total_laid_off) 'Number of employees laid off'
from layoffs_staging2
group by company
order by sum(total_laid_off) desc
limit 10;

-- Total number of years.
select max(`date`), min(`date`)
from layoffs_staging2;

-- Top 10 industries laid off maximum number of employees
select industry, sum(total_laid_off) 
'Number of employees laid off'
from layoffs_staging2
group by 1
order by 2 desc
limit 10;

-- Top 10 countries laid off maximum number of employees
select country, sum(total_laid_off) 'Number of employees laid off'
from layoffs_staging2
group by 1
order by 2 desc
limit 10;

-- Which Years has maximum number of laid off
select year(`date`), sum(total_laid_off) AS
'Number of employees laid off'
from layoffs_staging2
group by 1
order by 2 desc
limit 10;

-- Which stage has maximum number of laid off
select stage, sum(total_laid_off) AS
'Number of employees laid off'
from layoffs_staging2
group by 1
order by 2 desc;

-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

-- now use it in a CTE so we can query off of it
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, 
SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) 
OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;

-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year.

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, 
  SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, 
  DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) 
  AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 5
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;


