import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.{SparkConf, SparkContext}
import java.io.PrintWriter

/**
  * Created by vsinha on 3/20/2018.
  */
object houseDelayPrediction {

  // 定义 house 类，表示航班信息
  case class house(house_name: String, 
                    shi: String,
                    ting: String,
                    mianji: Double,
                    louceng: String, 
                    xiaoqu_name: String,
                    xiaoqu_weizhi0: String,
                    xiaoqu_weizhi1: String, 
                    xiaoqu_weizhi2: String,
                    guanjianci: String,
                    danjia: Double
                    )

  // 解析 CSV 行数据为 house 对象
  def parsehouse(str: String): house = {
    val line = str.split(",")
    house(line(0), 
          line(1), 
          line(2), 
          line(3).toDouble, 
          line(4), 
          line(5), 
          line(6), 
          line(7), 
          line(8), 
          line(9),
          line(10).toDouble
          )
  }

  // 对单价进行分类处理
  def danjia_fenlei(str: Double): Double = {
    if(str < 600.0) 0.0
      else if(str < 800.0) 1.0
      else if(str < 1000.0) 2.0
      else if(str < 1200.0) 3.0
      else if(str < 1400.0) 4.0
      else if(str < 1600.0) 5.0
      else if(str < 1800.0) 6.0
      else if(str < 2000.0) 7.0
      else if(str < 2200.0) 8.0
      else if(str < 2400.0) 9.0
      else if(str < 2600.0) 10.0
      else if(str < 2800.0) 11.0
      else if(str < 3000.0) 12.0
      else if(str < 3500.0) 13.0
      else if(str < 4000.0) 14.0
      else if(str < 4500.0) 15.0
      else if(str < 5000.0) 16.0
      else if(str < 5500.0) 17.0
      else if(str < 6000.0) 18.0
      else if(str < 7000.0) 19.0
      else if(str < 8000.0) 20.0
      else if(str < 9000.0) 21.0
      else if(str < 10000.0) 22.0
      else if(str < 15000.0) 23.0
      else if(str < 20000.0) 24.0
      else if(str < 25000.0) 25.0
      else if(str < 30000.0) 26.0
      else 27.0
  }

  def main(args: Array[String]) {

    // 设置 Hadoop 主目录，这里使用 Windows 路径
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    // 配置 Spark 应用
    val sparkConf = new SparkConf()
      .setAppName("houseDelayPrediction")
      .setMaster("local[2]")

    // 创建 Spark 上下文
    val sc = new SparkContext(sparkConf)

    // 读取 CSV 文件
    val data = sc.textFile("src/main/resources/ajk_tianjin_zufang_1.csv")
    // 创建一个PrintWriter对象，将结果输出到文件
    val writer = new PrintWriter("ajk_tianjin_zufang_output.txt")

    // 移除文件头
    val header = data.first()
    val textRDD = data.filter(row => row != header)

    // 转换为 house 对象的 RDD，并缓存
    val housesRDD = textRDD.map(parsehouse).cache()

    // 下一步是将非数字特征转换为数字值

    // 将非数字小区位置第一个值转换为数字值
    var xiaoqu_weizhi0Map: Map[String, Int] = Map()
    var index0: Int = 0
    housesRDD.map(house => house.xiaoqu_weizhi0).distinct.collect.foreach(x => { xiaoqu_weizhi0Map += (x -> index0); index0 += 1 })
    
    // 将非数字小区位置二个值转换为数字值
    var xiaoqu_weizhi1Map: Map[String, Int] = Map()
    var index1: Int = 0
    housesRDD.map(house => house.xiaoqu_weizhi1).distinct.collect.foreach(x => { xiaoqu_weizhi1Map += (x -> index1); index1 += 1 })

    // 将非数字小区名字值转换为数字值
    var xiaoqu_nameMap: Map[String, Int] = Map()
    var index2: Int = 0
    housesRDD.map(house => house.xiaoqu_name).distinct.collect.foreach(x => { xiaoqu_nameMap += (x -> index2); index2 += 1 })

    // 将非数字装修转换为数字值
    var house_nameMap: Map[String, Int] = Map()
    var index4: Int = 0
    housesRDD.map(house => house.house_name).distinct.collect.foreach(x => { house_nameMap += (x -> index4); index4 += 1 })

    // 将非数字朝向转换为数字值
    var loucengMap: Map[String, Int] = Map()
    var index5: Int = 0
    housesRDD.map(house => house.louceng).distinct.collect.foreach(x => { loucengMap += (x -> index5); index5 += 1 })

    // 将非数字未知1转换为数字值
    var xiaoqu_weizhi2Map: Map[String, Int] = Map()
    var index6: Int = 0
    housesRDD.map(house => house.xiaoqu_weizhi2).distinct.collect.foreach(x => { xiaoqu_weizhi2Map += (x -> index6); index6 += 1 })

    // 将非数字用水类型转换为数字值
    var shiMap: Map[String, Int] = Map()
    var index7: Int = 0
    housesRDD.map(house => house.shi).distinct.collect.foreach(x => { shiMap += (x -> index7); index7 += 1 })

    // 将非数字用电类型转换为数字值
    var tingMap: Map[String, Int] = Map()
    var index8: Int = 0
    housesRDD.map(house => house.ting).distinct.collect.foreach(x => { tingMap += (x -> index8); index8 += 1 })

    // 将非数字关键词转换为数字值
    var guanjianciMap: Map[String, Int] = Map()
    var index13: Int = 0
    housesRDD.map(house => house.guanjianci).distinct.collect.foreach(x => { guanjianciMap += (x -> index13); index13 += 1 })

    // 创建特征数组
    val mlprep = housesRDD.map(house => { 
      // 小区第一个位置 // 类别
      val xiaoqu_weizhi01 = xiaoqu_weizhi0Map(house.xiaoqu_weizhi0)
      // 小区第二个位置 // 类别
      val xiaoqu_weizhi11 = xiaoqu_weizhi1Map(house.xiaoqu_weizhi1)
      // 小区名字 // 类别
      val xiaoqu_name1 = xiaoqu_nameMap(house.xiaoqu_name)
      // 面积
      val mianji1 = house.mianji
      // 装修 // 类别
      val house_name1 = house_nameMap(house.house_name)
      // 朝向 // 类别
      val louceng1 = loucengMap(house.louceng)
      // 未知1
      val xiaoqu_weizhi21 = xiaoqu_weizhi2Map(house.xiaoqu_weizhi2)
      // 用水类型 // 类别
      val shi1 = shiMap(house.shi) 
      // 用电类型 // 类别
      val ting1 = tingMap(house.ting) 
      // 单价
      val danjia1 = danjia_fenlei(house.danjia)  
      // 关m, k键词 // 类别
      val guanjianci1 = guanjianciMap(house.guanjianci)
      
      Array(danjia1.toDouble,
            xiaoqu_weizhi01.toDouble, 
            xiaoqu_weizhi11.toDouble, 
            xiaoqu_name1.toDouble,
            mianji1.toDouble, 
            house_name1.toDouble, 
            louceng1.toDouble, 
            xiaoqu_weizhi21.toDouble, 
            shi1.toDouble, 
            ting1.toDouble,
            guanjianci1.toDouble
            )
    })

    // 创建标记点
    // 第一个参数是标签或目标变量，本例中为单价和总价
    // 第二个参数是特征向量
    val mldata = mlprep.map(x => LabeledPoint(x(0), Vectors.dense(x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10))))

    // 将数据拆分为训练集和测试集

    // 将mldata拆分为训练集和测试集
    val splits = mldata.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // 接下来是训练模型

    /* categoricalFeaturesInfo指定哪些特征是分类特征以及每个特征可以取多少个分类值。
    这是一个从特征索引到该特征的类别数的映射。*/
    var categoricalFeaturesInfo = Map[Int, Int]()

    // 在决策树中，numClasses 主要用于分类问题，因为它指定了目标变量的类别数量。
    // numClasses 被设置为1，因为连续型特征的预测不涉及多个离散的类别。
    // 如果你的问题是回归问题，其中目标是一个连续的数值，而不是分类，那么 numClasses 应该设置为1。
    val numClasses = 28
    // 表示用于计算节点纯度的方法。常见的选择包括 "gini" 和 "entropy"。
    // "gini" 使用基尼系数来计算纯度，"entropy" 使用信息熵来计算纯度。
    val impurity = "entropy"
    // 表示决策树的最大深度
    val maxDepth = 16
    // 表示连续特征离散化时的最大箱数
    val maxBins = 7000

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    // 测试模型
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    // 打印决策树
    writer.println(model.toDebugString)

    // 输出前20个误差不超过1的预测结果到文件
    writer.println("预测结果：")
    val closePredictions = labelAndPreds.filter {
      case (label, prediction) => math.abs(label - prediction) <= 1
    }
    closePredictions.take(20).foreach {
      case (label, prediction) => writer.println(s"真实值：$label, 预测值：$prediction")
    }

    // 输出所有误差不超过1的预测结果的个数
    val totalClosePredictions = closePredictions.count()
    writer.println("Correct Percentage: " + (totalClosePredictions.toDouble / testData.count()) * 100)

    // 关闭文件写入流
    writer.close()
  }
}
