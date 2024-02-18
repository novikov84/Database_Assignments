-- Проверяем исходные данные на наличие пропусков по ключевым значениям
select * from customer where customer_id is null;
select * from transaction where customer_id is null;

-- Проверим для всех ли записей customer_id в transaction есть соответствующее ключевое значение в customer
select t.*
from transaction t
left join customer c on  t.customer_id = c.customer_id
where c.customer_id is null;
-- Есть проблема с customer_id 5034, его нет в таблице customer, а в transaction для него есть три транзакции

-- проверим, есть ли дубликаты в таблице с клиентами
select customer_id, count(*) as cnt
FROM customer
GROUP BY customer_id
HAVING count(*) > 1;

-- проверим, есть ли дубликаты в таблице с транзакциями.
select transaction_id, count(*) as cnt
FROM transaction
GROUP BY transaction_id
HAVING count(*) > 1;

  
-- Вывести распределение (количество) клиентов по сферам деятельности, отсортировав результат по убыванию количества.
select job_industry_category, count(*) as number_of_clients
from customer3
group by job_industry_category
order by number_of_clients desc;

-- Найти сумму транзакций за каждый месяц по сферам деятельности, отсортировав по месяцам и по сфере деятельности. 
select 
  date_trunc('month',to_date(t.transaction_date, 'DD.MM.YYYY')) as month,  
  c.job_industry_category,
  sum(t.list_price) as total_transactions
from transaction3 t
join customer3 c on t.customer_id = c.customer_id
group by month, c.job_industry_category
order by month, c.job_industry_category;

-- Альтернативный вариант того же запроса:
select 
  extract(month from to_date(t.transaction_date, 'DD.MM.YYYY')) as month,
  c.job_industry_category,
  sum(t.list_price) as total_transactions
from transaction3 t
join customer3 c on t.customer_id = c.customer_id
group by month, c.job_industry_category
order by month, c.job_industry_category;

-- Вывести количество онлайн-заказов для всех брендов в рамках подтвержденных заказов клиентов из сферы IT. 
select 
  t.brand, 
  count(*) as orders
from transaction3 t
join customer3 c on t.customer_id = c.customer_id
where c.job_industry_category = 'IT'
  and t.online_order = 'True'
  and t.order_status = 'Approved'
group by t.brand
order by orders desc;


-- Найти по всем клиентам сумму всех транзакций (list_price), максимум, минимум и количество транзакций, отсортировав результат по убыванию суммы транзакций и количества транзакций. Выполните двумя способами: используя только group by и используя только оконные функции. Сравните результат.

-- Используем group by
select 
  c.customer_id,
  sum(t.list_price) as sum_transactions,
  max(t.list_price) as max_transaction,
  min(t.list_price) as min_transaction,
  count(t.transaction_id) as transaction_count
from customer3 c
join transaction3 t on c.customer_id = t.customer_id
group by c.customer_id
order by sum_transactions desc, transaction_count desc;

-- Используем оконные функции (чтобы была видна разница, выведем и transaction_id)
select distinct
   c.customer_id,
   t.transaction_id,
   sum(t.list_price) over (partition by c.customer_id) as sum_transactions,
   max(t.list_price) over (partition by c.customer_id) as max_transaction,
   min(t.list_price) over (partition by c.customer_id) as min_transaction,
   count(t.transaction_id) over (partition by c.customer_id) as transaction_count
from customer3 c
join transaction3 t on c.customer_id = t.customer_id
order by sum_transactions desc, transaction_count desc;

-- Разница очевидна - в случае использовани оконных функций можно вывести все транзакции для каждого клиента, одновременно добавив результаты оконных функций


-- Найти имена и фамилии клиентов с минимальной/максимальной суммой транзакций за весь период (сумма транзакций не может быть null). Напишите отдельные запросы для минимальной и максимальной суммы. 
-- Для максимальной суммы:
select c.customer_id, c.first_name, c.last_name, sum(list_price) as sum_transactions
from customer3 c
join transaction3 t on c.customer_id = t.customer_id
group by c.customer_id, c.first_name, c.last_name
order by sum_transactions desc
limit 1;

-- Для минимальной суммы:
select c.customer_id, c.first_name, c.last_name, sum(list_price) as sum_transactions
from customer3 c
join transaction3 t on c.customer_id = t.customer_id
group by c.customer_id, c.first_name, c.last_name
having sum(list_price) > 0
order by sum_transactions asc
limit 1;


-- Вывести только самые первые транзакции клиентов. Решить с помощью оконных функций.
select *
from (
  select 
    t.*,
    row_number() over (partition by t.customer_id order by t.transaction_date asc) as rn
  from transaction3 t
) as subquery
where rn = 1;


-- Вывести имена, фамилии и профессии клиентов, между транзакциями которых был максимальный интервал (интервал вычисляется в днях)
with transaction_intervals as (
    select
        t.customer_id,
        to_date(t.transaction_date, 'DD.MM.YYYY') - lag(to_date(t.transaction_date, 'DD.MM.YYYY')) over (partition by t.customer_id order by to_date(t.transaction_date, 'DD.MM.YYYY')) as day_interval
    from transaction3 t
),
ranked_intervals as (
    select
        customer_id,
        day_interval,
        rank() over (order by day_interval desc) as interval_rank
    from transaction_intervals
    where day_interval is not null
)
select
	c.first_name,
    c.last_name,
    c.job_title,
    ri.day_interval
from customer3 c
join ranked_intervals ri on c.customer_id = ri.customer_id
where ri.interval_rank <= 5 -- выведем 5 клиентов с максимальным интервалом
order by ri.day_interval desc;

