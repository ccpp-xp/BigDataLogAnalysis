
import matplotlib.pyplot as plt
from pyspark.ml.feature import PCA
from pyspark.sql.functions import col
def vision(predictions):

    pandas_df = predictions.toPandas()
    cluster_features = {}
    # 对每个聚类，我们将找出最常见的请求方法、URL、状态码和用户代理
    plt.figure(figsize=(8, 6))
    for cluster in range(5):
        clustered_data = pandas_df[pandas_df['cluster'] == cluster]
        cluster_features[cluster] = {
            'most_common_request_method': clustered_data['request_method'].mode()[0],
            'most_common_status': clustered_data['status'].mode()[0],
            'most_common_user_agent': clustered_data['user_agent'].mode()[0],
            'top_urls': clustered_data['url'].value_counts().head(5).to_dict()  # 取前5个最常见的URLs
        }
    print(cluster_features)

    # 进行 PCA 降维，将特征空间降到2维
    pca = PCA(k=2, inputCol="features", outputCol="pcaFeatures")
    model = pca.fit(predictions)

    # 应用 PCA 转换
    result = model.transform(predictions).select("pcaFeatures", "prediction")

    # 抽样，对数据进行降采样以便在本地进行可视化
    sampled_data = result.sample(False, 0.1).toPandas()

    # 使用 Matplotlib 进行可视化
    fig, ax = plt.subplots()
    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']  # 颜色列表

    for i in range(5):  # k=5，所以迭代 5 次
        cluster_data = sampled_data[sampled_data['prediction'] == i]
        pca_features = cluster_data['pcaFeatures'].apply(lambda x: x.toArray())
        x = pca_features.apply(lambda x: x[0])
        y = pca_features.apply(lambda x: x[1])
        ax.scatter(x, y, c=colors[i % len(colors)], label=f'Cluster {i}', alpha=0.5)

    plt.savefig('/home/cluster_visualization.png')

    ax.legend()
    plt.show()


