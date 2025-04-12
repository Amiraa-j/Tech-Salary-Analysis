create database global_tech_salary;
use global_tech_salary;
-- data cleaning
select *,
row_number()over(partition by work_year, experience_level, employment_type, job_title, salary, 
salary_currency, salary_in_usd, employee_residence, remote_ratio, company_location, company_size) as row_num
from globalsalary
;
with ctes as(select *,
row_number()over(partition by work_year, experience_level, employment_type, job_title, salary, 
salary_currency, salary_in_usd, employee_residence, remote_ratio, company_location, company_size) as row_num
from globalsalary
)
delete 
from ctes
where row_num>1
;
CREATE TABLE `globalsalaries` (
  `work_year` int DEFAULT NULL,
  `experience_level` text,
  `employment_type` text,
  `job_title` text,
  `salary` int DEFAULT NULL,
  `salary_currency` text,
  `salary_in_usd` int DEFAULT NULL,
  `employee_residence` text,
  `remote_ratio` int DEFAULT NULL,
  `company_location` text,
  `company_size` text,
  `row_num` int default null
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into `globalsalaries` 
select *,
row_number()over(partition by work_year, experience_level, employment_type, job_title, salary, 
salary_currency, salary_in_usd, employee_residence, remote_ratio, company_location, company_size) as row_num
from globalsalary
;
delete
from `globalsalaries`
where row_num>1
;
alter table `globalsalaries` 
drop column row_num
;

update globalsalaries
set experience_level = "Entry_level"
where experience_level like "%EN%";

update globalsalaries
set experience_level = "Senior_level"
where experience_level like "%SE%";

update globalsalaries
set experience_level = "Mid_level"
where experience_level like "%MI%";

update globalsalaries
set experience_level = "Executive_level"
where experience_level like "%Ex%";

update globalsalaries
set employment_type = "Full_Time"
where employment_type like "%FT%";

update globalsalaries
set employment_type = "Contract"
where employment_type like "%CT%";

update globalsalaries
set employment_type = "Part_Time"
where employment_type like "%PT%";

update globalsalaries
set employment_type = "Freelance"
where employment_type like "%FL%";

update globalsalaries
set job_title = "BI Analyst"
where job_title = "BI Data Analyst";

update globalsalaries
set job_title = "Data Analytics Lead "
where job_title = "Data Analyst Lead";

update globalsalaries
set job_title = "Data Modeler "
where job_title = "Data Modeller";

update globalsalaries
set job_title = "Data Scientist "
where job_title = "Data Science";

update globalsalaries
set job_title = "Finance Data Analyst "
where job_title = "Financial Data Analyst";

update globalsalaries
set job_title = "Machine Learning Engineer "
where job_title = "ML Engineer";

update globalsalaries
set job_title = "ML Ops Engineer"
where job_title = "MLOps Engineer";

update globalsalaries
set job_title = "Data Science Lead "
where job_title = "Lead Data Scientist";

update globalsalaries
set job_title = "Business Intelligence Analyst"
where job_title = "Business Intelligence";

alter table globalsalaries
modify remote_ratio  varchar (255)
;
update globalsalaries
set remote_ratio = "Fully remote"
where remote_ratio = 100
;
update globalsalaries
set remote_ratio = "Hybrid "
where remote_ratio = '50'
;
update globalsalaries
set remote_ratio = "On-Site"
where remote_ratio = '0';

update globalsalaries
set company_size= "Large"
where company_size  = 'L'
;
update globalsalaries
set company_size= "Small"
where company_size  = 'S'
;
update globalsalaries
set company_size= "Medium"
where company_size  = 'M'
;
select *
from `globalsalaries`
;

-- EDA
select work_year,round(avg(salary_in_usd),2) as Avg_Total_Salary
from globalsalaries
group by work_year
;
select  work_year,count(*) Total_employees
from globalsalaries
group by  work_year;


select employment_type,round(avg(salary_in_usd),2) as Avg_Total_Salary
from globalsalaries
group by employment_type
order by 2 desc
;
select experience_level,work_year,avg(salary_in_usd)
from globalsalaries
group by experience_level,work_year
;
select experience_level,
round(avg(case when work_year='2020' then (salary_in_usd) end),2) as '2020',
round(avg(case when work_year='2021' then (salary_in_usd) end),2) as '2021',
round(avg(case when work_year='2022' then (salary_in_usd) end),2)as '2022',
round(avg(case when work_year='2023' then (salary_in_usd) end),2) as '2023',
round(avg(case when work_year='2024' then (salary_in_usd) end),2)as '2024'
from globalsalaries
group by experience_level
;
select employment_type,count(*)employee_count ,round(avg(salary_in_usd),2) as Avg_Total_Salary
from globalsalaries
group by employment_type
order by 2 desc
;

select employment_type,max(salary_in_usd),min(salary_in_usd)
from globalsalaries
group by employment_type
order by 2 desc
;
select experience_level,count(*)employees_count, round(avg(salary_in_usd),2) as Avg_Total_Salary
from globalsalaries
group by experience_level
order by 3 desc
;

select round(avg(salary_in_usd),2) as Global_Avg_Total_Salary 
from globalsalaries
;
select sum(salary_in_usd)
from globalsalaries
;
with general_average as(
 select round(avg(salary_in_usd),2) as Global_Avg_Total_Salary 
from globalsalaries),

above_average_salary as (select experience_level, employment_type,round(avg(salary_in_usd),2) as avg_salary
from globalsalaries
group by experience_level, employment_type
)
select above_average_salary.experience_level,above_average_salary.employment_type , avg_salary
from above_average_salary 
cross join general_average
where above_average_salary .avg_salary >  general_average. Global_Avg_Total_Salary 
;
select job_title, avg(salary_in_usd) as average_salary
from globalsalaries
group by job_title
order by 2 desc
limit 10
;
select job_title, avg(salary_in_usd) as average_salary
from globalsalaries
group by job_title
order by 2 asc
limit 10
;

WITH least_paying_job_title AS (
    -- Get the bottom 10 job titles based on average salary
    SELECT job_title, AVG(salary_in_usd) AS average_salary
    FROM globalsalaries
    GROUP BY job_title
    ORDER BY average_salary ASC
    LIMIT 10
)
SELECT 
    lpjt.job_title,
   coalesce( Round(AVG(CASE WHEN gs.work_year = '2020' THEN gs.salary_in_usd END),2),0) AS "2020",
   coalesce(round(AVG(CASE WHEN gs.work_year = '2021' THEN gs.salary_in_usd END),2),0) AS "2021",
   coalesce(round(AVG(CASE WHEN gs.work_year = '2022' THEN gs.salary_in_usd END),2),0) AS "2022",
    coalesce(round(AVG(CASE WHEN gs.work_year = '2023' THEN gs.salary_in_usd END),2),0) AS "2023",
    coalesce(round(AVG(CASE WHEN gs.work_year = '2024' THEN gs.salary_in_usd END),2),0) AS "2024"
FROM least_paying_job_title lpjt
JOIN globalsalaries gs
    ON lpjt.job_title = gs.job_title
GROUP BY lpjt.job_title
ORDER BY lpjt.job_title;

select employee_residence,avg(salary_in_usd) AS average_salary
from globalsalaries
group by employee_residence
order by 2 desc
limit 10
;
select employee_residence,avg(salary_in_usd) AS average_salary
from globalsalaries
group by employee_residence
order by 2 asc
limit 10
;
select remote_ratio,avg(salary_in_usd) AS average_salary
from globalsalaries
group by remote_ratio
order by 2 desc
;
with rank_ctes as(
select 
rank()over( order by avg(salary_in_usd) desc) as rank_by_location,
company_location,avg(salary_in_usd)
from globalsalaries
group by company_location)
select *
from rank_ctes
where rank_by_location <= 10
;
select company_size, avg(salary_in_usd) 
from globalsalaries
group by company_size
order by 2 desc
;