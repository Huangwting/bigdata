from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col
import matplotlib.pyplot as plt

# 创建Spark会话
spark = SparkSession.builder.appName("HouseClustering").getOrCreate()

# 读取二手房数据
data = spark.read.csv("/input/test_area_price_street.csv", header=True, inferSchema=True)

# 使用StringIndexer将街道名称编码成数字标签，并设置处理NULL值
indexer = StringIndexer(inputCol="所在街道", outputCol="street_index", handleInvalid="skip")
data = indexer.fit(data).transform(data)

# 删除包含NULL值的行
data = data.dropna()


# 选择需要的特征列，这里假设有面积、价格和街道三个特征
feature_columns = ["面积", "价格", "street_index"]
vec_assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data = vec_assembler.transform(data)

# K-means模型训练，假设要分成5类
kmeans = KMeans(k=5, seed=1)
model = kmeans.fit(data)
data = model.transform(data)

# 将聚类结果保存到CSV文件
data.select("面积", "价格", "所在街道", "prediction").write.csv("clustered_results.csv", header=True)

# 可视化聚类结果
centers = model.clusterCenters()
plt.scatter(data.toPandas()["面积"], data.toPandas()["价格"], c=data.toPandas()["prediction"])
plt.scatter([center[0] for center in centers], [center[1] for center in centers], marker="x", s=200, linewidths=3, color='r')
plt.xlabel("面积")
plt.ylabel("价格")
plt.title("二手房数据聚类结果")
plt.show()

# 关闭Spark会话
spark.stop()

