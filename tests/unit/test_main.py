import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch

from scripts.main import extract


@pytest.fixture(scope="session")
def spark_session(request):
    spark = (SparkSession
             .builder
             .master('local')
             .appName('unittest')
             .getOrCreate())

    return spark


@patch("pyspark.sql.SparkSession.read")
def test_extract_should_pass_when_read_data_from_dir(mock_read, spark_session):
    mock_source_file_path = "/path/file.csv"
    mock_df = spark_session.createDataFrame([(1, "foo"), (2, "bar")], ["id", "name"])
    mock_read.csv.return_value = mock_df
    df = extract(mock_source_file_path)

    assert mock_read.csv.call_count == 1
    assert df.count() == mock_df.count()


@patch('os.path.exists')
def test_extract_should_raise_exception_when_no_data_from_dir(mock_exists, spark_session):
    mock_source_file_path = "path/not_exists.csv"
    mock_exists.return_value = False
    with pytest.raises(Exception):
        extract(mock_source_file_path)
