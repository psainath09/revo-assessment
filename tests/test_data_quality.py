def test_no_negative_prices():
    df = spark.table("housing.silver.airbnb_clean")
    assert df.filter("price <= 0").count() == 0

def test_gold_table_exists():
    tables = spark.sql("SHOW TABLES IN housing.gold").collect()
    assert any(row.tableName == "postcode_metrics" for row in tables)