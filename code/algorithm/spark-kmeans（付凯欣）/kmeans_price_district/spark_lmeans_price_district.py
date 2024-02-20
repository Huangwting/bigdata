from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.clustering import KMeans
import matplotlib.pyplot as plt

# 创建Spark会话
spark = SparkSession.builder.appName("HouseClustering").getOrCreate()

# 读取二手房数据
data = spark.read.csv("/input/test_price_district.csv", header=True, inferSchema=True)

# 使用StringIndexer将所在市区名称编码成数字标签
indexer = StringIndexer(inputCol="区", outputCol="district_index")
data = indexer.fit(data).transform(data)

# 选择需要的特征列，这里假设有价格和所在市区两个特征
feature_columns = ["价格", "district_index"]
vec_assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data = vec_assembler.transform(data)

# K-means模型训练，假设要分成5类
kmeans = KMeans(k=5, seed=1)
model = kmeans.fit(data)
data = model.transform(data)

# 将聚类结果保存到CSV文件
data.select("价格", "区", "prediction").write.csv("clustered_results.csv", header=True)

# 可视化聚类结果
centers = model.clusterCenters()
plt.scatter(data.toPandas()["价格"], data.toPandas()["district_index"], c=data.toPandas()["prediction"])
plt.scatter([center[0] for center in centers], [center[1] for center in centers], marker="x", s=200, linewidths=3, color='r')
plt.xlabel("price")
plt.ylabel("district")
plt.title("result")
plt.show()

# 关闭Spark会话
spark.stop()
