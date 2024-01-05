from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql import Row
from pyspark import SparkContext
from cluster_vision import vision
import re
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == '__main__':

    sc = SparkContext(appName='cluster')
    # 加载数据
    logs_rdd = sc.textFile('hdfs://spark0:9000/log/')
    log_pattern = re.compile(
        r'(?P<ip>\d+\.\d+\.\d+\.\d+)\s+-\s+-\s+\[(?P<time>.+?)\]\s+"'
        r'(?P<request_method>\w+)\s+(?P<url>\S+)\s+(?P<protocol>HTTP\/\d\.\d)"\s+'
        r'(?P<status>\d+)\s+(?P<bytes_sent>\d+)\s+"(?P<referer>\S*)"\s+"'
        r'(?P<user_agent>.+?)"\s*\Z'
    )


    # 定义一个解析每行日志的函数
    def parse_log_line(line):
        match = log_pattern.match(line)
        if match:
            return (match.group('ip'),
                    match.group('url'),
                    match.group('request_method'),
                    match.group('referer'),
                    match.group('user_agent'))
        else:
            return (None, None, None, None, None)

    # 应用解析函数到每一行日志
    parsed_logs_rdd = logs_rdd.map(parse_log_line)
    # 过滤掉无效的日志行
    valid_logs_rdd = parsed_logs_rdd.filter(lambda x: x[0] is not None)
    # 定义 DataFrame 的 schema
    schema = StructType([
        StructField("ip", StringType(), True),
        StructField("url", StringType(), True),
        StructField("request_method", StringType(), True),
        StructField("referer", StringType(), True),
        StructField("user_agent", StringType(), True)
    ])
    # 初始化 SparkSession
    spark = SparkSession.builder.appName("ClusterExample").getOrCreate()

    # 将 valid_logs_rdd 转换为 DataFrame
    df = spark.createDataFrame(valid_logs_rdd, schema)

    # 使用 StringIndexer 将分类特征转换为索引
    indexers = [StringIndexer(inputCol=column, outputCol=column + "_index").fit(df) for column in list(set(df.columns))]

    # 使用 VectorAssembler 将索引特征合并到一个向量中
    vec_assembler = VectorAssembler(inputCols=[indexer.getOutputCol() for indexer in indexers], outputCol="features")
    df_vec = vec_assembler.transform(df)

    # 标准化特征
    scaler = StandardScaler(inputCol='features', outputCol='scaledFeatures')
    scaler_model = scaler.fit(df_vec)
    df_scaled = scaler_model.transform(df_vec)

    # 训练 K-Means 模型
    kmeans = KMeans(featuresCol='scaledFeatures', k=5)  # k 是聚类数
    model = kmeans.fit(df_scaled)

    # 获取聚类结果
    predictions = model.transform(df_scaled)
    # predictions.select('prediction').show()
    vision(predictions)
    spark.stop()
