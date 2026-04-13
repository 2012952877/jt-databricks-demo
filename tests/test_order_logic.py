# 文件路径: tests/test_order_logic.py
import pytest
from pyspark.sql import SparkSession
from src.order_cleanup import clean_orders

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_order_cleanup_removes_nulls(spark):
    """测试用例：确保清洗逻辑真的过滤掉了空状态的订单"""
    mock_data = [("MX-01", "PAID"), ("MX-02", None)]
    df = spark.createDataFrame(mock_data, ["order_id", "status"])
    
    result_df = clean_orders(df)
    
    # 断言：过滤后应该只剩 1 条数据
    assert result_df.count() == 1
    assert result_df.collect()[0]["order_id"] == "MX-01"