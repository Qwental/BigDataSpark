# BigDataSpark

### Выполнил Бугренков Владимир М8О-311Б-23

Структура репозитория:
```text
~/BigDataSpark> tree -la                                            
.
├── .dockerignore
├── .env # Я специально его запушил, чтобы все норм запускалось =)
├── .env.example
├── docker-compose.yml
├── download_jars.sh
├── jars
│   ├── clickhouse-jdbc-0.6.3-all.jar
│   └── postgresql-42.7.3.jar
├── README.md
├── spark
│   ├── etl.py # etl пайплайн
│   ├── run.sh # скрипт для запуска спарка
│   └── spark.log # логи спарка
└── sql
    └── init.sql # перенос csv в mock_data_table

```


## Инструкция
### 1. git clone

```bash
git clone https://github.com/Qwental/BigDataSpark.git
cd BigDataSpark
```

####  если вдруг что-то не так с jar-файлами их можно скачать

```bash
chmod +x download_jars.sh
./download_jars.sh
```

### 2. Запуск

```bash
docker compose up -d
```

`spark-submit` запускается автоматически

логи спарка:
```bash
docker logs -f spark-submit
```

ждем
```text 
...
ETL completed.
```

### 3. Проверка PostgreSQL и звезды

```bash
docker exec -it PG_sparklab psql -U admin -d db_postgres
```

### 3.1 Список таблиц звезды
```sql
\dt star_*
```

### 3.2 Количество строк
```sql
SELECT 'star_fact_sales'    AS tbl, COUNT(*) AS cnt FROM star_fact_sales
UNION ALL
SELECT 'star_dim_customer'  AS tbl, COUNT(*) AS cnt FROM star_dim_customer
UNION ALL
SELECT 'star_dim_product'   AS tbl, COUNT(*) AS cnt FROM star_dim_product
UNION ALL
SELECT 'star_dim_store'     AS tbl, COUNT(*) AS cnt FROM star_dim_store
UNION ALL
SELECT 'star_dim_supplier'  AS tbl, COUNT(*) AS cnt FROM star_dim_supplier
UNION ALL
SELECT 'star_dim_date'      AS tbl, COUNT(*) AS cnt FROM star_dim_date;
```

### 3.3 запрос с джоином для проверки связи по ключам
```sql
SELECT
    dc.customer_first_name,
    dc.customer_last_name,
    dp.product_name,
    ds.store_name,
    f.sale_quantity,
    f.sale_total_price
FROM star_fact_sales    f
JOIN star_dim_customer  dc ON f.customer_id  = dc.customer_id
JOIN star_dim_product   dp ON f.product_id   = dp.product_id
JOIN star_dim_store     ds ON f.store_id     = ds.store_id
LIMIT 10;
```

Выход из psql:
```sql
\q
```

## 4. Проверка ClickHouse Витрины

```bash
docker exec -it db_clickhouse clickhouse-client \
  --user admin \
  --password password
```

### 4.1 Список всех таблиц и вьюх
```sql
SHOW TABLES;
```

Ожидаемый вывод — 6 таблиц + 18 вьюх:


### 4.2 Количество строк в базовых таблицах
```sql
SELECT 'base_products'  AS tbl, count(*) AS cnt FROM base_products
UNION ALL
SELECT 'base_customers', count(*) FROM base_customers
UNION ALL
SELECT 'base_time',      count(*) FROM base_time
UNION ALL
SELECT 'base_stores',    count(*) FROM base_stores
UNION ALL
SELECT 'base_suppliers', count(*) FROM base_suppliers
UNION ALL
SELECT 'base_quality',   count(*) FROM base_quality;
```



### 4.3 Витрина 1 — Продукты
Рейтинг продуктов по количеству продаж
```sql
SELECT * FROM v_prod_by_sales LIMIT 10;
```
Выручка по категориям
```sql
SELECT * FROM v_prod_revenue_by_category;
```
Рейтинг и отзывы по продуктам
```sql
SELECT * FROM v_prod_rating_stats LIMIT 10;
```


### 4.4 Витрина 2 — Клиенты
Клиенты по общей сумме покупок
```sql
SELECT * FROM v_cust_by_total_spent LIMIT 10;
```
Распределение по странам
```sql
SELECT * FROM v_cust_by_country LIMIT 10;
```
Средний чек
```sql
SELECT * FROM v_cust_avg_check LIMIT 10;
```


### 4.5 Витрина 3 — Время
Месячные тренды
```sql
SELECT * FROM v_time_monthly_trends;
```
Годовые тренды
```sql
SELECT * FROM v_time_yearly_trends;
```
Средний заказ по месяцам
```sql
SELECT * FROM v_time_avg_order_by_month;
```


### 4.6 Витрина 4 — Магазины
Магазины по выручке
```sql
SELECT * FROM v_store_by_revenue LIMIT 10;
```
Распределение по городам и странам
```sql
SELECT * FROM v_store_by_geo LIMIT 10;
```
Средний чек по магазинам
```sql
SELECT * FROM v_store_avg_check LIMIT 10;
```


### 4.7 Витрина 5 — Поставщики
Поставщики по выручке
```sql
SELECT * FROM v_supp_by_revenue LIMIT 10;
```
Средняя цена товаров
```sql
SELECT * FROM v_supp_avg_price LIMIT 10;
```
Распределение по странам
```sql
SELECT * FROM v_supp_by_country LIMIT 10;
```



### 4.8 Витрина 6 — Качество
Продукты с наивысшим рейтингом
```sql
SELECT * FROM v_qual_highest_rated;
```
Продукты с наименьшим рейтингом
```sql
SELECT * FROM v_qual_lowest_rated;
```
Продукты с наибольшим количеством отзывов
```sql
SELECT * FROM v_qual_most_reviewed;
```


Выход
```sql
\q
```