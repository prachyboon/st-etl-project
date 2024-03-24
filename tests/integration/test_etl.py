import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from scripts.main import extract, transform_fav_product

FILE_PATH = "integration/fixture/test_transaction.csv"


@pytest.fixture(scope="session")
def spark_session(request):
    spark = (SparkSession
             .builder
             .master('local')
             .appName('unittest')
             .getOrCreate())

    return spark


def test_extract_should_pass_when_read_data_from_dir():
    df = extract(FILE_PATH)
    assert df.count() == 4


def test_transform_fav_product_should_pass_with_expected_result(spark_session):
    data = [(10001, "DETA800"), (10007, "PURA500")]
    cols = ["custId", "favourite_product"]
    schema = StructType(
        [StructField("custId", IntegerType()),
         StructField("favourite_product", StringType())]
    )
    expect_fav_df = spark_session.createDataFrame(data, cols, schema)

    df = extract(FILE_PATH)
    fav_df = transform_fav_product(df)

    assert fav_df.count() == 2
    assert fav_df.count() == expect_fav_df.count()
    assert fav_df.collect() == expect_fav_df.collect()
