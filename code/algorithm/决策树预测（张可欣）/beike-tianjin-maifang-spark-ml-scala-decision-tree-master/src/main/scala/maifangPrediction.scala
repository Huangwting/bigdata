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
  case class house(ID: String,
                    xiaoqu_weizhi0: String,
                    xiaoqu_weizhi1: String, 
                    xiaoqu_name: String, 
                    zongjia: Double, 
                    danjia: Double, 
                    mianji: Double, 
                    huxing: String,
                    huxing_jiegou: String, 
                    chaoxiang: String, 
                    zhuangxiu: String, 
                    xiaoqu_junjia: String, 
                    xiaoqu_shijian: String,
                    xiaoqu_leixing: String, 
                    xiaoqu_cengshu: String
                    )

  // 解析 CSV 行数据为 house 对象
  def parsehouse(str: String): house = {
    val line = str.split(",")
    house(line(0), 
          line(1), 
          line(2), 
          line(3), 
          line(4).toDouble, 
          line(5).toDouble, 
          line(6).toDouble, 
          line(7), 
          line(8), 
          line(9),
          line(10), 
          line(11), 
          line(12), 
          line(13),
          line(14)
          )
  }

  // 对单价进行分类处理
  def danjia_fenlei(str: Double): Double = {
    if(str < 500.0) 0.0
      else if(str < 2500.0) 1.0
      else if(str < 4500.0) 2.0
      else if(str < 6500.0) 3.0
      else if(str < 8500.0) 4.0
      else if(str < 10500.0) 5.0
      else if(str < 12500.0) 6.0
      else if(str < 14500.0) 7.0
      else if(str < 16500.0) 8.0
      else if(str < 18500.0) 9.0
      else if(str < 20500.0) 10.0
      else if(str < 25500.0) 11.0
      else if(str < 30500.0) 12.0
      else if(str < 35500.0) 13.0
      else if(str < 40500.0) 14.0
      else if(str < 45500.0) 15.0
      else if(str < 50500.0) 16.0
      else if(str < 60500.0) 17.0
      else if(str < 70500.0) 18.0
      else if(str < 80500.0) 19.0
      else if(str < 90500.0) 20.0
      else if(str < 100500.0) 21.0
      else if(str < 120500.0) 22.0
      else if(str < 140500.0) 23.0
      else if(str < 160500.0) 24.0
      else if(str < 180500.0) 25.0
      else if(str < 200500.0) 26.0
      else 27.0
  }

  // 对单价分类机型恢复
  def zhuanhuan(str: Double): Double = {
    if(str < 500.0) 0.0
      else if(str < 2500.0) 1.0
      else if(str < 4500.0) 2.0
      else if(str < 6500.0) 3.0
      else if(str < 8500.0) 4.0
      else if(str < 10500.0) 5.0
      else if(str < 12500.0) 6.0
      else if(str < 14500.0) 7.0
      else if(str < 16500.0) 8.0
      else if(str < 18500.0) 9.0
      else if(str < 20500.0) 10.0
      else if(str < 25500.0) 11.0
      else if(str < 30500.0) 12.0
      else if(str < 35500.0) 13.0
      else if(str < 40500.0) 14.0
      else if(str < 45500.0) 15.0
      else if(str < 50500.0) 16.0
      else if(str < 60500.0) 17.0
      else if(str < 70500.0) 18.0
      else if(str < 80500.0) 19.0
      else if(str < 90500.0) 20.0
      else if(str < 100500.0) 21.0
      else if(str < 120500.0) 22.0
      else if(str < 140500.0) 23.0
      else if(str < 160500.0) 24.0
      else if(str < 180500.0) 25.0
      else if(str < 200500.0) 26.0
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
    val data = sc.textFile("src/main/resources/beike_tianjin_ershoufang_shang_1.csv")
    // 创建一个PrintWriter对象，将结果输出到文件
    val writer = new PrintWriter("beike_tianjin_ershoufang_output.txt")

    // 转换为 house 对象的 RDD，并缓存
    val housesRDD = data.map(parsehouse).cache()

    // 下一步是将非数字特征转换为数字值

    // 将非数字小区位置第一个值转换为数字值
    var xiaoqu_weizhi0Map: Map[String, Int] = Map()
    var index0: Int = 0
    housesRDD.map(house => house.xiaoqu_weizhi0).distinct.collect.foreach(x => { xiaoqu_weizhi0Map += (x -> index0); index0 += 1 })
    
    // 将非数字小区位置二个值转换为数字值
    var xiaoqu_weizhi1Map: Map[String, Int] = Map()
    var index10: Int = 0
    housesRDD.map(house => house.xiaoqu_weizhi1).distinct.collect.foreach(x => { xiaoqu_weizhi1Map += (x -> index10); index10 += 1 })

    // 将非数字小区名字值转换为数字值
    var xiaoqu_nameMap: Map[String, Int] = Map()
    var index1: Int = 0
    housesRDD.map(house => house.xiaoqu_name).distinct.collect.foreach(x => { xiaoqu_nameMap += (x -> index1); index1 += 1 })

    // 将非数字户型转换为数字值
    var huxingMap: Map[String, Int] = Map()
    var index2: Int = 0
    housesRDD.map(house => house.huxing).distinct.collect.foreach(x => { huxingMap += (x -> index2); index2 += 1 })

    // 将非数字户型结构转换为数字值
    var huxing_jiegouMap: Map[String, Int] = Map()
    var index3: Int = 0
    housesRDD.map(house => house.huxing_jiegou).distinct.collect.foreach(x => { huxing_jiegouMap += (x -> index3); index3 += 1 })

    // 将非数字朝向转换为数字值
    var chaoxiangMap: Map[String, Int] = Map()
    var index4: Int = 0
    housesRDD.map(house => house.chaoxiang).distinct.collect.foreach(x => { chaoxiangMap += (x -> index4); index4 += 1 })

    // 将非数字装修转换为数字值
    var zhuangxiuMap: Map[String, Int] = Map()
    var index5: Int = 0
    housesRDD.map(house => house.zhuangxiu).distinct.collect.foreach(x => { zhuangxiuMap += (x -> index5); index5 += 1 })

    // 将非数字小区均价转换为数字值
    var xiaoqu_junjiaMap: Map[String, Int] = Map()
    var index6: Int = 0
    housesRDD.map(house => house.xiaoqu_junjia).distinct.collect.foreach(x => { xiaoqu_junjiaMap += (x -> index6); index6 += 1 })

    // 将非数字小区建造时间转换为数字值
    var xiaoqu_shijianMap: Map[String, Int] = Map()
    var index7: Int = 0
    housesRDD.map(house => house.xiaoqu_shijian).distinct.collect.foreach(x => { xiaoqu_shijianMap += (x -> index7); index7 += 1 })

    // 将非数字小区楼房类型转换为数字值
    var xiaoqu_leixingMap: Map[String, Int] = Map()
    var index8: Int = 0
    housesRDD.map(house => house.xiaoqu_leixing).distinct.collect.foreach(x => { xiaoqu_leixingMap += (x -> index8); index8 += 1 })

    // 将非数字小区层数转换为数字值
    var xiaoqu_cengshuMap: Map[String, Int] = Map()
    var index9: Int = 0
    housesRDD.map(house => house.xiaoqu_cengshu).distinct.collect.foreach(x => { xiaoqu_cengshuMap += (x -> index9); index9 += 1 })

    // 创建特征数组
    val mlprep = housesRDD.map(house => { 
      // 小区第一个位置 // 类别
      val xiaoqu_weizhi01 = xiaoqu_weizhi0Map(house.xiaoqu_weizhi0)
      // 小区第二个位置 // 类别
      val xiaoqu_weizhi11 = xiaoqu_weizhi1Map(house.xiaoqu_weizhi1)
      // 小区名字 // 类别
      val xiaoqu_name1 = xiaoqu_nameMap(house.xiaoqu_name)
      // 总价,因为想着预测了单价相当于预测了总价，就先删了
      // val zongjia1 = house.zongjia
      // 单价
      val danjia1 = danjia_fenlei(house.danjia)
      // val danjia1 =house.danjia
      // 面积
      val mianji1 = house.mianji
      // 户型 // 类别
      val huxing1 = huxingMap(house.huxing)
      // 户型结构 // 类别
      val huxing_jiegou1 = huxing_jiegouMap(house.huxing_jiegou) 
      // 朝向 // 类别
      val chaoxiang1 = chaoxiangMap(house.chaoxiang) 
      // 装修 // 类别
      val zhuangxiu1 = zhuangxiuMap(house.zhuangxiu) 
      // 小区均价
      val xiaoqu_junjia1 = xiaoqu_junjiaMap(house.xiaoqu_junjia) 
      // 小区建造时间 // 类别
      val xiaoqu_shijian1 = xiaoqu_shijianMap(house.xiaoqu_shijian) 
      // 小区楼房类型 // 类别
      val xiaoqu_leixing1 = xiaoqu_leixingMap(house.xiaoqu_leixing) 
      // 小区层数 // 类别
      val xiaoqu_cengshu1 = xiaoqu_cengshuMap(house.xiaoqu_cengshu)
      
      Array(danjia1.toDouble,
            xiaoqu_weizhi01.toDouble, 
            xiaoqu_weizhi11.toDouble, 
            xiaoqu_name1.toDouble,
            mianji1.toDouble, 
            huxing1.toDouble,
            huxing_jiegou1.toDouble,
            chaoxiang1.toDouble, 
            zhuangxiu1.toDouble, 
            xiaoqu_junjia1.toDouble, 
            xiaoqu_shijian1.toDouble, 
            xiaoqu_leixing1.toDouble,
            xiaoqu_cengshu1.toDouble
            )
    })

    // 创建标记点
    // 第一个参数是标签或目标变量，本例中为单价和总价
    // 第二个参数是特征向量
    val mldata = mlprep.map(x => LabeledPoint(x(0), Vectors.dense(x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12))))

    // 将数据拆分为训练集和测试集

    // 我们没有是和非两种选择，因此我们不做加强错误集的操作
    // mldata0是85%的未延误航班
    // val mldata0 = mldata.filter(x => x.label == 0).randomSplit(Array(0.85, 0.15))(0)
    // mldata1是100%的延误航班
    // val mldata1 = mldata.filter(x => x.label != 0)
    // mldata2是延误和未延误的混合
    // val mldata2 = mldata0 ++ mldata1

    // 将mldata拆分为训练集和测试集
    val splits = mldata.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // 接下来是训练模型

    /* categoricalFeaturesInfo指定哪些特征是分类特征以及每个特征可以取多少个分类值。
    这是一个从特征索引到该特征的类别数的映射。*/
    var categoricalFeaturesInfo = Map[Int, Int]()
    // 如果所有特征都是连续特征就不用以下部分
    // 箭头左边是特征索引，箭头右边是分类特征的类别数量
    // 小区第一个位置的数量
    // categoricalFeaturesInfo += (1 -> xiaoqu_weizhi0Map.size)
    // 小区第二个位置的数量
    // categoricalFeaturesInfo += (2 -> xiaoqu_weizhi1Map.size)
    // 小区名字的数量
    // categoricalFeaturesInfo += (3 -> xiaoqu_nameMap.size)
    // 小区户型的数量
    // categoricalFeaturesInfo += (5 -> huxingMap.size)
    // 小区户型结构的数量
    // categoricalFeaturesInfo += (6 -> huxing_jiegouMap.size)
    // 房子朝向的数量
    // categoricalFeaturesInfo += (7 -> chaoxiangMap.size)
    // 房子装修的数量
    // categoricalFeaturesInfo += (8 -> zhuangxiuMap.size)
    // 小区建造时间的数量
    // categoricalFeaturesInfo += (10 -> xiaoqu_shijianMap.size)
    // 小区楼房类型的数量
    // categoricalFeaturesInfo += (11 -> xiaoqu_leixingMap.size)
    // 小区层数的数量
    // categoricalFeaturesInfo += (12 -> xiaoqu_cengshuMap.size)

    

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
