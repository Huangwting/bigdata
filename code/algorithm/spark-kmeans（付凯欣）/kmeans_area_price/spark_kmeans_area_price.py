from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
import matplotlib.pyplot as plt

# 创建SparkSession
spark = SparkSession.builder \
    .appName("HouseClustering") \
    .getOrCreate()

# 读取二手房数据
data = spark.read.csv("test1.csv", header=True, inferSchema=True)

# 将面积和价格合并为一个特征向量
assembler = VectorAssembler(inputCols=["area", "price"], outputCol="features")
data = assembler.transform(data)

# 训练K-means模型
kmeans = KMeans().setK(3).setSeed(1)
model = kmeans.fit(data)

# 添加聚类标签到数据
clustered_data = model.transform(data)

# 输出文本分类结果
result_file_path = "result.csv"
clustered_data.select("area", "price", "prediction").write.csv(result_file_path, header=True)

# 可视化结果
centers = model.clusterCenters()

plt.figure(figsize=(8, 6))
plt.scatter(clustered_data.toPandas()['area'], clustered_data.toPandas()['price'], c=clustered_data.toPandas()['prediction'], cmap='viridis', alpha=0.5)
plt.scatter([center[0] for center in centers], [center[1] for center in centers], c='red', marker='x', label='Centroids')
plt.xlabel('Area')
plt.ylabel('Price')
plt.title('House Clustering')
plt.legend()
plt.show()

# 关闭SparkSession
spark.stop()
