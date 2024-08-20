-- Data Cleaning

select *
from layoffs;

-- Remove Duplicates
-- Standardize the Data
-- Null Values or Blank Values
-- Remove Any Columns

-- Make raw data as a backup and create same new table where we do changes.

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select * 
from layoffs;

-- Remove Duplicates using row_number

select *,
row_number() over(partition by 
company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;

with duplicate_ctc as
(
	select *,
	row_number() over(partition by 
	company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
	from layoffs_staging
)
select *
from duplicate_ctc
where row_num > 1;

-- row_num is not updatable because it is not present in raw data. (Create new table where row_num column is present)
-- click layoffs_staging table -> copy to clickboard -> create statement -> crtl + v (paste it)
-- change table's name layoffs_staging to layoffs_staging2
-- add row_num in table column with int datatype

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * 
from layoffs_staging2;

-- Insert rows in layoffs_staging2

insert into layoffs_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

-- Remove duplicates

delete
from layoffs_staging2
where row_num > 1;

select * 
from layoffs_staging2
where row_num > 1;

-- Standardizing data
-- Trim company column
select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

-- Resolve alignment error in Industry column
select distinct industry
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry like 'Crypto%'
order by industry;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select *
from layoffs_staging2
where location like 'DÃ¼sseldorf'
order by location;

update layoffs_staging2
set location = 'Dusseldorf'
where location like 'DÃ¼sseldorf%';

select *
from layoffs_staging2
where location like 'FlorianÃ³polis'
order by location;

update layoffs_staging2
set location = 'Florianopolis'
where location like 'FlorianÃ³polis%';

select *
from layoffs_staging2
where location like 'MalmÃ¶'
order by location;

update layoffs_staging2
set location = 'Malmo'
where location like 'MalmÃ¶%';

select distinct location
from layoffs_staging2;

select `date`, str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;

select *
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

update layoffs_staging2
set industry = NULL 
where industry = '';

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company = 'Airbnb';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;
 
update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select distinct industry
from layoffs_staging;

select *
from layoffs_staging2
where company = 'Bally\'s Interactive';
 
-- Droping Data

select *
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

Alter table layoffs_staging2
drop column row_num;

select * 
from layoffs_staging2;