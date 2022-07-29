import pytest
from dbt.clients.jinja import MacroGenerator
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

@pytest.mark.parametrize(
    "macro_generator", ["macro.spark_utils.get_tables"], indirect=True
)

@pytest.fixture
def spark_session():

    sc = SparkContext.getOrCreate()
    session = SparkSession \
                .builder \
                .config(sc) \
                .master("local[1]") \
                .appName("pytest") \
                .getOrCreate()
    return session


@pytest.fixture
def jaffle_orders_query():
    expected_table = "dev_dbt_rafael.stg_jaffle_shop_orders"
    query = f"""
        CREATE TABLE {expected_table}
        (
            order_id int,
            customer_id int,
            order_date timestamp,
            status string
        )
    """

def test_create_table(
    spark_session: SparkSession, 
    macro_generator: MacroGenerator,
    jaffle_orders_query
) -> None:

    spark_session.sql(jaffle_orders_query)
    tables = macro_generator()
    assert tables == ["dev_dbt_rafael.stg_jaffle_shop_orders"]

#The macro can be a part of a query too, for example:

"""import pytest
from dbt.clients.jinja import MacroGenerator
from pyspark.sql import SparkSession

@pytest.mark.parametrize(
    "macro_generator",
    ["macro.my_project.to_cents"],
    indirect=True,
)
def test_dollar_to_cents(
    spark_session: SparkSession, macro_generator: MacroGenerator
) -> None:
    expected = spark_session.createDataFrame([{"cents": 1000}])
    to_cents = macro_generator("price")
    out = spark_session.sql(
        "with data AS (SELECT 10 AS price) "
        f"SELECT cast({to_cents} AS bigint) AS cents FROM data"
    )
    assert out.collect() == expected.collect()
"""