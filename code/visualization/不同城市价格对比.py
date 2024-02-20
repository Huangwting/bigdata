import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 读取Excel文件
file_path = 'C:\\Users\\11298\\Desktop\\beike20.xlsx'
data = pd.read_excel(file_path)
plt.rcParams['font.sans-serif']=['SimHei']

# 使用groupby和max获取每个城市的最高单价和总价
max_prices = data.groupby('城市').agg({'单价': 'max', '总价': 'max'}).reset_index()

# 设置图表
fig, ax1 = plt.subplots(figsize=(10, 6))

# 使用seaborn绘制柱状图（单价）
sns.barplot(x='城市', y='单价', data=max_prices, ax=ax1, color='blue', label='最高单价')

# 设置第二个y轴，用于绘制折线图（总价）
ax2 = ax1.twinx()
ax2.plot(max_prices['城市'], max_prices['总价'], color='green', marker='o', label='最高总价')

# 设置图表标题和轴标签
ax1.set_title('每个城市的最高单价和总价对比')
ax1.set_xlabel('城市')
ax1.set_ylabel('最高单价', color='blue')
ax2.set_ylabel('最高总价', color='green')

# 添加图例
ax1.legend(loc='upper left')
ax2.legend(loc='upper right')

# 显示图表
plt.show()
