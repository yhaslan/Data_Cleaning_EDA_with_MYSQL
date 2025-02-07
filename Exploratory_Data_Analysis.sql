-- EXPLORATORY DATA ANALYSIS

SELECT * 
FROM layoffs_staging2;

SELECT SUM(total_laid_off), MAX(total_laid_off), AVG(total_laid_off), MAX(percentage_laid_off), AVG(percentage_laid_off)
FROM layoffs_staging2;
# there seems to be at least one company which laid of all of its employees at a given date

SELECT company,percentage_laid_off
FROM layoffs_staging2
WHERE percentage_laid_off = 1; # 116 rows returned

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC; 

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;
# Amazon is the company with the highest number of layoffs across the time

SELECT MIN(layoff_date), MAX(layoff_date)
FROM layoffs_staging2;
# between 11 march 2020 and 6 march 2023


SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;
# consumer and retail industries seem most affected

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;
# highest number of layoffs are from the US

SELECT YEAR(layoff_date), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(layoff_date)
ORDER BY 2 DESC;
# 2022 is the highest


# Let's look at the progression of layoffs, in other word rolling sums

SELECT SUBSTRING(layoff_date, 6,2) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `MONTH`
ORDER BY 1 ASC;

#because we formatted the month as date, this is equivalent to:
SELECT MONTH(layoff_date), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY MONTH(layoff_date)
ORDER BY 1 ASC;

#but let's get year and month:
SELECT SUBSTRING(layoff_date, 1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(layoff_date, 1,7) IS NOT NULL # this line gives error when we use the alias month
GROUP BY `MONTH`
ORDER BY 1 ASC;


WITH Rolling_total AS (
	SELECT SUBSTRING(layoff_date, 1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
	FROM layoffs_staging2
	WHERE SUBSTRING(layoff_date, 1,7) IS NOT NULL 
	GROUP BY `MONTH`
	ORDER BY 1 ASC
)
SELECT `MONTH`,total_off, SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total
;

# Company ordered by number of layoffs per year

SELECT company,YEAR(layoff_date), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(layoff_date)
ORDER BY 1 ASC;

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company,YEAR(layoff_date), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(layoff_date)
)
SELECT * , DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
ORDER BY Ranking ASC ;  # will show each year's top layoff-ers, then second-top etc..

# OR for a more meaningul table:

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company,YEAR(layoff_date), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(layoff_date)
),
Company_Ranking AS (
SELECT * , DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Ranking
WHERE Ranking <= 5;
# this will enlist top 5 of each year

