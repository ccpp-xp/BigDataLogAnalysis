import requests
import json
import re
from tqdm import tqdm
from pyspark.sql import SparkSession
from ip_analysis_vision import vision

if __name__ == '__main__':
    print("正在获取数据")
    li = []
    # 创建一个 SparkSession
    spark = SparkSession.builder.appName("ip_analysis").getOrCreate()
    # 定义日志文件路径
    file_path = "hdfs://spark0:9000/log/*.txt"
    # 读取日志文件为 DataFrame
    log_data = spark.read.text(file_path)
    # 用于解析ip的地址的接口
    url = 'http://whois.pconline.com.cn/ipJson.jsp'
    # 使用正则表达式匹配出所有的 IP 地址，得到一个列表
    ip_addresses = log_data.rdd.flatMap(lambda row: re.findall(r'(\d+\.\d+\.\d+\.\d+)', row[0]))
    # 计算了前1000个ip的结果
    count = 1000
    for ip in tqdm(ip_addresses.collect()):
        count -= 1
        if count == 0:
            break
        # 构造接口的参数：ip内容、json格式
        param = {'ip': ip,
                 'json': 'true'
                 }
        try:
            # 查询 IP 地址的位置信息
            response = requests.get(url, params=param)
            ip_info = json.loads(response.text.replace("\\", " "))

            # 将 IP 地址的位置信息存入结果列表
            li.append(ip_info)
        except json.decoder.JSONDecodeError:
            print("解析 JSON 错误")
            continue
    spark.stop()
    vision(li)