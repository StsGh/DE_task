
-- 1) Создать структуру БД, наполнить тестовыми данными.


--создаем схему с основными клиентскими данными
CREATE SCHEMA DB_DEFAULT;
ALTER SCHEMA DB_DEFAULT DEFAULT INCLUDE SCHEMA PRIVILEGES;
CREATE ROLE DB_DEFAULT_R;
CREATE ROLE DB_DEFAULT_W;
CREATE ROLE DB_DEFAULT_A;
GRANT USAGE ON SCHEMA DB_DEFAULT TO DB_DEFAULT_R, DB_DEFAULT_W;
GRANT CREATE, USAGE, DROP ON SCHEMA DB_DEFAULT TO DB_DEFAULT_A;
GRANT SELECT ON SCHEMA DB_DEFAULT TO DB_DEFAULT_R;
GRANT UPDATE, DELETE, TRUNCATE, INSERT ON SCHEMA DB_DEFAULT TO DB_DEFAULT_W;
GRANT CREATE, SELECT, INSERT, UPDATE, TRUNCATE, DELETE, REFERENCES ON SCHEMA DB_DEFAULT TO DB_DEFAULT_A;

--создаем таблицу с данными по клиентам
CREATE TABLE IF NOT EXISTS DB_DEFAULT.tb_users(
	uid IDENTITY (1,1) ,
	registration_date DATE ,
	country VARCHAR(256) ,
	name VARCHAR(256) ,
	surname VARCHAR(256) ,
	birth_date DATE ,
	PRIMARY KEY (uid)
)
ORDER BY registration_date
UNSEGMENTED ALL NODES
;

--загружаем тестовые данные
COPY DB_DEFAULT.tb_users (
  registration_date ,
  country ,
  name ,
  surname ,
  birth_date
)
FROM LOCAL '/Users/ghavrilchenko/Documents/DE_test/users.csv'
DELIMITER ';'
ABORT ON ERROR;


--создаем таблицу с данными по счетам
CREATE TABLE IF NOT EXISTS DB_DEFAULT.tb_logins(
	user_uid INTEGER ,
	login VARCHAR(512) ,
	account_type VARCHAR(10) ,
	registration_date DATE,
	account_currency VARCHAR(10) ,
	close_date DATE ,
PRIMARY KEY (user_uid, login) ,
FOREIGN KEY (user_uid) REFERENCES DB_DEFAULT.tb_users (uid)
)
ORDER BY registration_date
UNSEGMENTED ALL NODES
;

--наполняем таблицу тестовыми данными
COPY DB_DEFAULT.tb_logins (
  user_uid ,
  login ,
  account_type ,
  registration_date ,
  account_currency
)
FROM LOCAL '/Users/ghavrilchenko/Documents/DE_test/logins.csv'
DELIMITER ';'
ABORT ON ERROR;

--создаем схему с данными по операциям
CREATE SCHEMA DB_BILLING;
ALTER SCHEMA DB_BILLING DEFAULT INCLUDE SCHEMA PRIVILEGES;
CREATE ROLE DB_BILLING_R;
CREATE ROLE DB_BILLING_W;
CREATE ROLE DB_BILLING_A;
GRANT USAGE ON SCHEMA DB_BILLING TO DB_BILLING_R, DB_BILLING_W;
GRANT CREATE, USAGE, DROP ON SCHEMA DB_BILLING TO DB_BILLING_A;
GRANT SELECT ON SCHEMA DB_BILLING TO DB_BILLING_R;
GRANT UPDATE, DELETE, TRUNCATE, INSERT ON SCHEMA DB_BILLING TO DB_BILLING_W;
GRANT CREATE, SELECT, INSERT, UPDATE, TRUNCATE, DELETE, REFERENCES ON SCHEMA DB_BILLING TO DB_BILLING_A;

--создаем таблицу с данными по операциям
CREATE TABLE IF NOT EXISTS DB_BILLING.tb_operations(
	transaction_id IDENTITY (1,1) ,
	login VARCHAR(512) ,
	operation_type VARCHAR(10) ,
	operation_date DATE ,
	amount_USD NUMERIC(18,5) ,
	PRIMARY KEY (transaction_id)
)
ORDER BY operation_date
SEGMENTED BY HASH(TRUNC(operation_date,'DD')) ALL NODES
;

--наполняем таблицу тестовыми данными
COPY DB_BILLING.tb_operations (
  login ,
  operation_type ,
  operation_date ,
  amount_USD
)
FROM LOCAL '/Users/ghavrilchenko/Documents/DE_test/operations.csv'
DELIMITER ';'
ABORT ON ERROR;

--создаем схему с данными по транзакциям
CREATE SCHEMA DB_ORDERSTAT;
ALTER SCHEMA DB_ORDERSTAT DEFAULT INCLUDE SCHEMA PRIVILEGES;
CREATE ROLE DB_BILLING_R;
CREATE ROLE DB_BILLING_W;
CREATE ROLE DB_BILLING_A;
GRANT USAGE ON SCHEMA DB_ORDERSTAT TO DB_ORDERSTAT_R, DB_ORDERSTAT_W;
GRANT CREATE, USAGE, DROP ON SCHEMA DB_ORDERSTAT TO DB_ORDERSTAT_A;
GRANT SELECT ON SCHEMA DB_ORDERSTAT TO DB_ORDERSTAT_R;
GRANT UPDATE, DELETE, TRUNCATE, INSERT ON SCHEMA DB_ORDERSTAT TO DB_ORDERSTAT_W;
GRANT CREATE, SELECT, INSERT, UPDATE, TRUNCATE, DELETE, REFERENCES ON SCHEMA DB_ORDERSTAT TO DB_ORDERSTAT_A;

--создаем таблицу с данными по транзакциям
CREATE TABLE IF NOT EXISTS DB_ORDERSTAT.tb_orders(
	order_id IDENTITY (1,1) ,
	login VARCHAR(512) ,
	order_close_date DATE ,
	order_amount NUMERIC(18,5) ,
	PRIMARY KEY (order_id)
)
ORDER BY order_close_date
SEGMENTED BY HASH(TRUNC(order_close_date,'DD')) ALL NODES
;

--наполняем таблицу тестовыми данными
COPY DB_ORDERSTAT.tb_orders (
  login ,
  order_close_date ,
  order_amount
)
FROM LOCAL '/Users/ghavrilchenko/Documents/DE_test/orders.csv'
DELIMITER ';'
ABORT ON ERROR;


/* 2) Написать запрос, который отобразит среднее время перехода пользователей между этапами воронки:
 - От регистрации до внесения депозита
 - От внесения депозита до первой сделки на реальном счёте
Только реальные счета
Учесть, что у пользователя может быть депозит, но не быть торговых операций
Период - последние 90 дней
Группировка - по странам
Сортировка - по убыванию количества пользователей
*/


select
       t1.country,
       count(t1.uid),
       avg(t2.first_operation - t1.registration_date) as first_operation_time,
       avg(t2.first_order - t1.registration_date) as first_order_time
from
(
  select
    uid,
    registration_date,
    country
    from DB_DEFAULT.tb_users
    where
    DATEDIFF(day,registration_date, current_date()) <= 90
    ) t1
left join
(
  select
    tbl_log.user_uid,
    tbl_log.login,
    tbl_log.registration_date,
    tbl_op.first_operation,
    tbl_ord.first_order
  from (
      select
           user_uid,
           login,
           registration_date
      from DB_DEFAULT.tb_logins
      where
            DATEDIFF(day,registration_date, current_date()) <= 90
        and (user_uid,registration_date) in
            (
                select user_uid,
                       min(registration_date) as registration_date
                from DB_DEFAULT.tb_logins
                where account_type = 'real'
                group by 1
                )
      ) as tbl_log
      left join
      (
          select
                 user_uid,
                 min(operation_date) as first_operation
          from DB_BILLING.tb_operations t01
              left join DB_DEFAULT.tb_logins t02 on t01.login=t02.login
          where operation_type = 'deposit'
          group by 1) as tbl_op on tbl_log.user_uid=tbl_op.user_uid
      left join
      (
          select
                 user_uid,
                 min(order_close_date) as first_order
          from DB_ORDERSTAT.tb_orders t01
              left join DB_DEFAULT.tb_logins t02 on t01.login=t02.login
          group by 1
          ) tbl_ord on tbl_log.user_uid=tbl_ord.user_uid
    ) t2 on t1.uid=t2.user_uid
group by 1
order by 1;


/* 3) Написать запрос, который отобразит количество всех клиентов по странам, у которых средний депозит >=1000
Вывод: country, количество клиентов в стране, количество клиентов у которых депозит >=1000 
*/


/* Решение будет значительно зависеть от методологии расчета среднего депозита.
   т.к. в условии задачи данные по суммам торговых операций на счету являются не существенными - не берем их в формулу расчета
   период для расчета выберем - весь lifetime клиента, среднемесячное значение возьмем за календарный месяц
   Для решения возьмем формулу расчета среднего депозита = среднее (пополнение лицевого счета - изьятия с депозита)
 */
 

select
       t6.country,
       count(distinct t6.uid) as total_users,
       count(distinct t8.user_uid) as margin_users
from
    (select
           t5.user_uid,
           avg(month_operation) as month_operation
    from (
             select
                    user_uid,
                    year_num,
                    month_num,
                    sum(month_operation) over (partition by t3.user_uid 
                      order by t3.year_num, t3.month_num rows 
                      between unbounded preceding and current row) as month_operation
             from (
                      select t4.user_uid,
                             sum(month_operation) as month_operation,
                             year_num,
                             month_num
                      from (
                               select login,
                                      operation_type,
                                      case operation_type
                                          when 'deposit' then sum(amount_USD)
                                          else sum(amount_USD) * (-1) end as month_operation,
                                      year(operation_date)                as year_num,
                                      month(operation_date)               as month_num
                               from DB_BILLING.tb_operations t1
                               group by 1, 2, 4, 5
                               order by 1
                           ) t2
                               left join (select user_uid, login from DB_DEFAULT.tb_logins) t4
                                         on t2.login = t4.login
                      group by 1, 3, 4
                  ) t3
         ) t5
        group by 1
        having avg(t5.month_operation) >=1000) t8
full join DB_DEFAULT.tb_users t6 on t8.user_uid=t6.uid
group by 1
;

/* 4) Написать запрос, который выводит первые 3 депозита каждого клиента.
Вывод: uuid, login, operation_date, порядковый номер депозита
*/

select
       t2.user_uid,
       t2.login,
       t3.operation_date,
       t2.rank
from
     (
         select
                *,
                ROW_NUMBER() over (partition by user_uid order by login) as rank
         from DB_DEFAULT.tb_logins t1
         ) t2
left join
         (
             select
                    login,
                    max(operation_date) as operation_date
             from DB_BILLING.tb_operations
             group by 1
             ) t3 on t2.login=t3.login
where rank <= 3
order by 1;