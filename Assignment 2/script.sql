-- Вывести все уникальные бренды, у которых стандартная стоимость выше 1500 долларов.
select distinct brand
from transaction
where standard_cost > 1500;

-- Вывести все подтвержденные транзакции за период '2017-04-01' по '2017-04-09' включительно.
select *
from transaction
where order_status = 'Approved'
	  and to_date(transaction_date, 'DD.MM.YYYY') between '2017-04-01' and '2017-04-09';

--Вывести все профессии у клиентов из сферы IT или Financial Services, которые начинаются с фразы 'Senior'.	 
select distinct job_title
from customer
where (job_industry_category = 'IT' or job_industry_category = 'Financial Services')
  and job_title like 'Senior%';

-- Вывести все бренды, которые закупают клиенты, работающие в сфере Financial Services
select distinct t.brand
from transaction t
join customer c on t.customer_id = c.customer_id
where c.job_industry_category = 'Financial Services';

-- Вывести 10 клиентов, которые оформили онлайн-заказ продукции из брендов 'Giant Bicycles', 'Norco Bicycles', 'Trek Bicycles'.
select c.customer_id, c.first_name, c.last_name, t.brand, t.online_order
from transaction t
join customer c on t.customer_id = c.customer_id
where t.brand in ('Giant Bicycles', 'Norco Bicycles', 'Trek Bicycles')
  and t.online_order = 'True'
limit 10;

-- Вывести всех клиентов, у которых нет транзакций.
select c.customer_id, c.first_name, c.last_name
from customer c
left join transaction t on c.customer_id = t.customer_id
where t.transaction_id is null;

-- Вывести всех клиентов из IT, у которых транзакции с максимальной стандартной стоимостью.
select distinct c.customer_id, c.first_name, c.last_name
from customer c
join transaction t on c.customer_id = t.customer_id
where c.job_industry_category = 'IT'
and t.standard_cost = (
    select max(standard_cost)
    from transaction
);
 
-- Вывести всех клиентов из сферы IT и Health, у которых есть подтвержденные транзакции за период '2017-07-07' по '2017-07-17'.
select c.customer_id, c.first_name, c.last_name
from customer c
join transaction t on c.customer_id = t.customer_id
where c.job_industry_category in ('IT', 'Health')
and t.order_status = 'Approved'
and to_date(t.transaction_date, 'DD.MM.YYYY') between '2017-07-07' and '2017-07-17'; 
 
 