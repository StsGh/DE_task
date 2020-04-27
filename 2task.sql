

---создадим таблицу с банковскими транзакциями
CREATE TABLE IF NOT EXISTS DB_DEFAULT.tb3_transactions(
	transaction_id IDENTITY (1,1) ,
    user_id INTEGER ,
	transaction_date DATE ,
	amount_USD NUMERIC(18,5) ,
	PRIMARY KEY (transaction_id)
)
ORDER BY transaction_date, user_id
SEGMENTED BY HASH(TRUNC(transaction_date,'DD')) ALL NODES
;

--создадим техническую таблицу, которая записывает отработанные процессы реконсиляции
CREATE TABLE IF NOT EXISTS DB_DEFAULT.tb3_jobs(
	job_id INTEGER ,
    match_type VARCHAR(256) , -- целевой признак: дата, значение, пользователь или совокупность признаков
	start_date TIMESTAMP ,
	row_count INTEGER ,
	PRIMARY KEY (job_id)
)
ORDER BY start_date
SEGMENTED BY HASH(TRUNC(start_date,'DD')) ALL NODES
;

--внесем первое значение
INSERT INTO DB_DEFAULT.tb3_jobs(job_id,match_type,start_date,row_count) VALUES (1,'numeric',now(),0);


-- таблица, в которую сохраняются нормализованные записи
CREATE TABLE IF NOT EXISTS DB_DEFAULT.tb3_transactions_norm(
    job_id INTEGER ,
	transaction_id INTEGER ,
    user_id INTEGER ,
	transaction_date DATE ,
	amount_USD NUMERIC(18,5) ,
	PRIMARY KEY (transaction_id)
)
ORDER BY job_id, transaction_id
SEGMENTED BY HASH(TRUNC(transaction_date,'DD')) ALL NODES
;

--таблица, в которую сохраняются несопоставленные записи
CREATE TABLE IF NOT EXISTS DB_DEFAULT.tb3_transactions_bad(
    job_id INTEGER ,
	transaction_id INTEGER ,
    user_id INTEGER ,
	transaction_date DATE ,
	amount_USD NUMERIC(18,5) ,
	PRIMARY KEY (transaction_id)
)
ORDER BY job_id, transaction_id
SEGMENTED BY HASH(TRUNC(transaction_date,'DD')) ALL NODES
;