import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.neighbors import KernelDensity

# 读取Excel文件
file_path = 'C:\\Users\\11298\\Desktop\\beike20.xlsx'
data = pd.read_excel(file_path)
plt.rcParams['font.sans-serif']=['SimHei']

plt.figure(figsize=(10, 6))

# 循环遍历各个区域
for region in df['区域'].unique():
    data = df[df['区域'] == region]['房屋总价']
    
    # 使用KernelDensity进行核密度估计
    kde = KernelDensity(bandwidth=5000, kernel='gaussian')
    kde.fit(data.values.reshape(-1, 1))
    
    # 生成一些测试数据点
    x = np.linspace(data.min(), data.max(), 1000).reshape(-1, 1)
    
    # 计算核密度估计值
    log_dens = kde.score_samples(x)
    
    # 绘制核密度估计曲线
    plt.plot(x, np.exp(log_dens), label=region)

plt.title('天津各区二手房房屋总价核密度估计')
plt.xlabel('房屋总价')
plt.ylabel('密度')
plt.legend()
plt.show()
