import os
import urllib.request
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    to_date,
    row_number,
    month,
    year,
    sum as _sum,
    avg as _avg,
    count as _count,
    coalesce,
    lit,
)
from pyspark.sql.types import StringType

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB = os.getenv("PG_DB", "db_postgres")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASS = os.getenv("PG_PASSWORD", "admin123")

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_USER = os.getenv("CH_USER", "admin")
CH_PASS = os.getenv("CH_PASSWORD", "password")

PG_URL = f"jdbc:postgresql://{PG_HOST}:5432/{PG_DB}"
PG_PROP = {"user": PG_USER, "password": PG_PASS, "driver": "org.postgresql.Driver"}

CH_URL = f"jdbc:clickhouse://{CH_HOST}:8123/default"
CH_PROP = {
    "user": CH_USER,
    "password": CH_PASS,
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "createTableOptions": "ENGINE = MergeTree() ORDER BY tuple()",
}
CH_HTTP = f"http://{CH_HOST}:8123/"


def ch_execute(sql: str):
    """
    DDL в ClickHouse по HTTP
    """
    try:
        data = sql.encode("utf-8")
        req = urllib.request.Request(CH_HTTP, data=data)
        req.add_header("X-ClickHouse-User", CH_USER)
        req.add_header("X-ClickHouse-Key", CH_PASS)
        req.add_header("Content-Type", "text/plain; charset=utf-8")
        with urllib.request.urlopen(req, timeout=30):
            pass
        print(f"[ClickHouse OK] {sql[:100].strip()}...")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"[ClickHouse ERROR {e.code}] {sql[:100].strip()}\n  >> {body}")
    except Exception as e:
        print(f"[ClickHouse ERROR] {sql[:100].strip()}\n  >> {e}")


def fill_nulls_for_ch(df):
    """
    Заменяем все null в строковых колонках на пустую строку
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, coalesce(col(field.name), lit("")))
    return df


def write_to_ch(df, table_name):
    """
    Записывает DataFrame в ClickHouse с заменой null
    """
    print(f"  Writing {table_name} to ClickHouse")
    clean_df = fill_nulls_for_ch(df)
    clean_df.write.jdbc(
        url=CH_URL, table=table_name, mode="overwrite", properties=CH_PROP
    )
    print(f"  [Writing OK] {table_name}")


# Основная логика пайплайна
def main():
    spark = (
        SparkSession.builder.appName("etl_lab2_nenahov")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("1) Reading raw data from PostgreSQL")
    raw = spark.read.jdbc(url=PG_URL, table="mock_data", properties=PG_PROP)
    clean = raw.withColumn("dt", to_date(col("sale_date"), "M/d/yyyy"))
    print(f"    Rows loaded: {clean.count()}")

    print("2) Building Star Schema dimensions")
    w_email = Window.orderBy("customer_email")
    w_prodname = Window.orderBy("product_name")
    w_store = Window.orderBy("store_name")
    w_supp = Window.orderBy("supplier_name")
    w_date = Window.orderBy("full_date")

    dim_customer = (
        clean.select(
            "customer_email",
            "customer_first_name",
            "customer_last_name",
            "customer_age",
            "customer_country",
            "customer_postal_code",
        )
        .dropDuplicates(["customer_email"])
        .withColumn("customer_id", row_number().over(w_email))
    )

    dim_product = (
        clean.select(
            "product_name",
            "product_category",
            "product_price",
            "product_rating",
            "product_reviews",
            "product_brand",
            "product_weight",
            "product_color",
            "product_size",
            "product_material",
        )
        .dropDuplicates(["product_name"])
        .withColumn("product_id", row_number().over(w_prodname))
    )

    dim_store = (
        clean.select(
            "store_name",
            "store_city",
            "store_state",
            "store_country",
            "store_phone",
            "store_email",
        )
        .dropDuplicates(["store_name"])
        .withColumn("store_id", row_number().over(w_store))
    )

    dim_supplier = (
        clean.select(
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_city",
            "supplier_country",
        )
        .dropDuplicates(["supplier_name"])
        .withColumn("supplier_id", row_number().over(w_supp))
    )

    dim_date = (
        clean.select(col("dt").alias("full_date"))
        .dropDuplicates(["full_date"])
        .withColumn("date_id", row_number().over(w_date))
        .withColumn("year", year("full_date"))
        .withColumn("month_num", month("full_date"))
    )

    print("3) Fact table building")
    fact = (
        clean.alias("f")
        .join(dim_customer.alias("c"), "customer_email")
        .join(dim_product.alias("p"), "product_name")
        .join(dim_store.alias("st"), "store_name")
        .join(dim_supplier.alias("sp"), "supplier_name")
        .join(dim_date.alias("d"), col("f.dt") == col("d.full_date"))
        .select(
            col("c.customer_id"),
            col("p.product_id"),
            col("st.store_id"),
            col("sp.supplier_id"),
            col("d.date_id"),
            col("f.sale_quantity"),
            col("f.sale_total_price"),
        )
    )

    print(f"    Fact rows: {fact.count()}")

    print("4) Star Schema to PostgreSQL")
    pg_tables = {
        "star_dim_customer": dim_customer,
        "star_dim_product": dim_product,
        "star_dim_store": dim_store,
        "star_dim_supplier": dim_supplier,
        "star_dim_date": dim_date,
        "star_fact_sales": fact,
    }

    for tbl_name, df in pg_tables.items():
        print(f"  Writing {tbl_name} ({df.count()} rows)...")
        df.write.jdbc(url=PG_URL, table=tbl_name, mode="overwrite", properties=PG_PROP)
        print(f"  [Star Schema OK] {tbl_name}")

    print("5) Writing denormalized base tables to ClickHouse")
    base_products = (
        fact.join(dim_product, "product_id")
        .join(dim_date, "date_id")
        .join(dim_customer, "customer_id")
        .select(
            col("product_id"),
            col("product_name"),
            col("product_category"),
            col("product_price"),
            col("product_rating"),
            col("product_reviews"),
            col("product_brand"),
            col("sale_quantity"),
            col("sale_total_price"),
            col("customer_id"),
            col("customer_country"),
            col("full_date"),
            col("year"),
            col("month_num"),
        )
    )

    base_customers = (
        fact.join(dim_customer, "customer_id")
        .join(dim_date, "date_id")
        .select(
            col("customer_id"),
            col("customer_first_name"),
            col("customer_last_name"),
            col("customer_email"),
            col("customer_country"),
            col("sale_quantity"),
            col("sale_total_price"),
            col("full_date"),
            col("year"),
            col("month_num"),
        )
    )

    base_time = fact.join(dim_date, "date_id").select(
        col("date_id"),
        col("full_date"),
        col("year"),
        col("month_num"),
        col("sale_quantity"),
        col("sale_total_price"),
    )

    base_stores = (
        fact.join(dim_store, "store_id")
        .join(dim_date, "date_id")
        .select(
            col("store_id"),
            col("store_name"),
            col("store_city"),
            col("store_state"),
            col("store_country"),
            col("sale_quantity"),
            col("sale_total_price"),
            col("full_date"),
            col("year"),
            col("month_num"),
        )
    )

    base_suppliers = (
        fact.join(dim_supplier, "supplier_id")
        .join(dim_product, "product_id")
        .join(dim_date, "date_id")
        .select(
            col("supplier_id"),
            col("supplier_name"),
            col("supplier_country"),
            col("supplier_city"),
            col("product_id"),
            col("product_name"),
            col("product_price"),
            col("product_category"),
            col("sale_quantity"),
            col("sale_total_price"),
            col("full_date"),
            col("year"),
            col("month_num"),
        )
    )

    base_quality = (
        fact.join(dim_product, "product_id")
        .groupBy(
            "product_id",
            "product_name",
            "product_category",
            "product_rating",
            "product_reviews",
            "product_brand",
        )
        .agg(
            _sum("sale_quantity").alias("total_qty"),
            _sum("sale_total_price").alias("total_revenue"),
            _count("*").alias("sales_count"),
        )
    )

    write_to_ch(base_products, "base_products")
    write_to_ch(base_customers, "base_customers")
    write_to_ch(base_time, "base_time")
    write_to_ch(base_stores, "base_stores")
    write_to_ch(base_suppliers, "base_suppliers")
    write_to_ch(base_quality, "base_quality")

    print("6) Creating VIEWs ClickHouse")
    all_views = [
        "v_prod_by_sales",
        "v_prod_revenue_by_category",
        "v_prod_rating_stats",
        "v_cust_by_total_spent",
        "v_cust_by_country",
        "v_cust_avg_check",
        "v_time_monthly_trends",
        "v_time_yearly_trends",
        "v_time_avg_order_by_month",
        "v_store_by_revenue",
        "v_store_by_geo",
        "v_store_avg_check",
        "v_supp_by_revenue",
        "v_supp_avg_price",
        "v_supp_by_country",
        "v_qual_highest_rated",
        "v_qual_lowest_rated",
        "v_qual_most_reviewed",
    ]
    for v in all_views:
        ch_execute(f"DROP VIEW IF EXISTS {v}")

    # Витрина 1: Продукты
    ch_execute(
        """
    CREATE VIEW v_prod_by_sales AS
    SELECT 
        product_name, 
        product_category, 
        SUM(sale_quantity) AS total_sold, 
        SUM(sale_total_price) AS total_revenue
    FROM base_products
    GROUP BY 
        product_name, 
        product_category
    ORDER BY total_sold DESC
    """
    )

    ch_execute(
        """
    CREATE VIEW v_prod_revenue_by_category AS
    SELECT 
        product_category, 
        SUM(sale_total_price) AS total_revenue, 
        COUNT(*) AS num_sales
    FROM base_products
    GROUP BY product_category
    ORDER BY total_revenue DESC
    """
    )

    ch_execute(
        """
    CREATE VIEW v_prod_rating_stats AS
    SELECT 
        product_name, 
        product_brand, 
        AVG(product_rating) AS avg_rating, 
        SUM(product_reviews) AS total_reviews
    FROM base_products
    GROUP BY 
        product_name, 
        product_brand
    ORDER BY avg_rating DESC
    """
    )

    # Витрина 2: Клиенты
    ch_execute(
        """
    CREATE VIEW v_cust_by_total_spent AS
    SELECT 
        customer_id, 
        customer_first_name, 
        customer_last_name, 
        customer_email, 
        SUM(sale_total_price) AS total_spent
    FROM base_customers
    GROUP BY 
        customer_id, 
        customer_first_name, 
        customer_last_name, 
        customer_email
    ORDER BY total_spent DESC
    """
    )

    ch_execute(
        """
    CREATE VIEW v_cust_by_country AS
    SELECT 
        customer_country, 
        COUNT(DISTINCT customer_id) AS num_customers, 
        SUM(sale_total_price) AS total_revenue
    FROM base_customers
    GROUP BY customer_country
    ORDER BY num_customers DESC
    """
    )

    ch_execute(
        """
    CREATE VIEW v_cust_avg_check AS
    SELECT 
        customer_id, 
        customer_first_name, 
        customer_last_name, 
        AVG(sale_total_price) AS avg_check
    FROM base_customers
    GROUP BY 
        customer_id, 
        customer_first_name, 
        customer_last_name
    ORDER BY avg_check DESC
    """
    )

    # Витрина 3: Время
    ch_execute(
        """
    CREATE VIEW v_time_monthly_trends AS
    SELECT 
        year, 
        month_num, 
        SUM(sale_total_price) AS monthly_revenue, 
        SUM(sale_quantity) AS monthly_qty, 
        COUNT(*) AS num_transactions
    FROM base_time
    GROUP BY 
        year, 
        month_num
    ORDER BY 
        year ASC, 
        month_num ASC
    """
    )

    ch_execute(
        """
    CREATE VIEW v_time_yearly_trends AS
    SELECT 
        year, 
        SUM(sale_total_price) AS yearly_revenue, 
        SUM(sale_quantity) AS yearly_qty, 
        AVG(sale_total_price) AS avg_order
    FROM base_time
    GROUP BY year
    ORDER BY year ASC
    """
    )

    ch_execute(
        """
    CREATE VIEW v_time_avg_order_by_month AS
    SELECT 
        month_num, 
        AVG(sale_total_price) AS avg_order_size, 
        AVG(sale_quantity) AS avg_qty
    FROM base_time
    GROUP BY month_num
    ORDER BY month_num ASC
    """
    )

    # Витрина 4: Магазины
    ch_execute(
        """
    CREATE VIEW v_store_by_revenue AS
    SELECT 
        store_name, 
        store_city, 
        store_country, 
        SUM(sale_total_price) AS total_revenue, 
        SUM(sale_quantity) AS total_qty
    FROM base_stores
    GROUP BY 
        store_name, 
        store_city, 
        store_country
    ORDER BY total_revenue DESC
    """
    )

    ch_execute(
        """
    CREATE VIEW v_store_by_geo AS
    SELECT 
        store_country, 
        store_city, 
        SUM(sale_total_price) AS total_revenue
    FROM base_stores
    GROUP BY 
        store_country, 
        store_city
    ORDER BY total_revenue DESC
    """
    )

    ch_execute(
        """
    CREATE VIEW v_store_avg_check AS
    SELECT 
        store_name, 
        store_city, 
        AVG(sale_total_price) AS avg_check, 
        COUNT(*) AS num_sales
    FROM base_stores
    GROUP BY 
        store_name, 
        store_city
    ORDER BY avg_check DESC
    """
    )

    # Витрина 5: Поставщики
    ch_execute(
        """
    CREATE VIEW v_supp_by_revenue AS
    SELECT 
        supplier_name, 
        supplier_country, 
        SUM(sale_total_price) AS total_revenue, 
        SUM(sale_quantity) AS total_qty
    FROM base_suppliers
    GROUP BY 
        supplier_name, 
        supplier_country
    ORDER BY total_revenue DESC
    """
    )

    ch_execute(
        """
    CREATE VIEW v_supp_avg_price AS
    SELECT 
        supplier_name, 
        AVG(product_price) AS avg_price
    FROM base_suppliers
    GROUP BY supplier_name
    ORDER BY avg_price DESC
    """
    )

    ch_execute(
        """
    CREATE VIEW v_supp_by_country AS
    SELECT 
        supplier_country, 
        SUM(sale_total_price) AS total_revenue
    FROM base_suppliers
    GROUP BY supplier_country
    ORDER BY total_revenue DESC
    """
    )

    # Витрина 6: Качество
    ch_execute(
        """
    CREATE VIEW v_qual_highest_rated AS
    SELECT 
        product_name, 
        product_category, 
        product_rating, 
        total_qty, 
        total_revenue
    FROM base_quality
    ORDER BY product_rating DESC
    """
    )

    ch_execute(
        """
    CREATE VIEW v_qual_lowest_rated AS
    SELECT 
        product_name, 
        product_category, 
        product_rating, 
        total_qty, 
        total_revenue
    FROM base_quality
    ORDER BY product_rating ASC
    """
    )

    ch_execute(
        """
    CREATE VIEW v_qual_most_reviewed AS
    SELECT 
        product_name, 
        product_brand, 
        product_reviews, 
        product_rating, 
        total_qty
    FROM base_quality
    ORDER BY product_reviews DESC
    """
    )

    print("     [OK]    All VIEWs created successfully")

    spark.stop()
    print("ETL completed")


if __name__ == "__main__":
    main()
