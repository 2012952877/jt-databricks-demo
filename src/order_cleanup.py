# src/order_cleanup.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# 💡 工程化改造：将核心清洗逻辑单独抽成一个函数，剥离对数据的直接依赖

def clean_orders(df):
    """
    清洗订单的核心逻辑：过滤空状态，打上时间戳
    """
    # ❌ 制造 BUG：假设有个粗心的程序员，直接把 .filter 这一步删掉了！
    # 现在它根本不过滤脏数据，直接给所有数据打上了时间戳
    clean_df = df.withColumn("processed_at", current_timestamp())
    return clean_df

def main():
    spark = SparkSession.builder.appName("JT_Order_Cleanup").getOrCreate()
    print("🚀 开始执行极兔墨西哥跨境订单清洗任务...")

    # 模拟读取原始数据
    data = [
        ("MX-1001", "PAID", 150.5, "MEXICO CITY"),
        ("MX-1002", "CANCELLED", 0.0, "MONTERREY"),
        ("MX-1003", "PAID", 99.9, "GUADALAJARA"),
        ("MX-1004", None, 50.0, None)
    ]
    raw_df = spark.createDataFrame(data, ["order_id", "status", "amount", "city"])
    
    # 调用抽离出来的清洗逻辑
    final_df = clean_orders(raw_df)
    
    final_df.show()
    print("🎉 任务执行成功，数据已准备好进行下游结算！")

if __name__ == "__main__":
    main()