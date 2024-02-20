import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# 读取CSV文件
file_path = r'C:\Users\11298\Desktop\combined_file.csv'
data = pd.read_csv(file_path)
plt.rcParams['font.sans-serif']=['SimHei']

# 将房屋总价列转换为数值型
data['总价'] = pd.to_numeric(data['总价'], errors='coerce')

# 删除包含NaN值的行
data = data.dropna(subset=['总价', '区'])

# 分箱处理，调整中间值的分块数
bins = pd.cut(data['总价'], bins=10)
data['总价分箱'] = bins

# 获取数量排名前几的区域
top_n_areas = data['区'].value_counts().head(10).index
filtered_data = data[data['区'].isin(top_n_areas)]

# 创建数据透视表
pivot_table = filtered_data.pivot_table(index='区', columns='总价分箱', aggfunc='size', fill_value=0)

# 获取价格范围
price_range = data['总价'].max() - data['总价'].min()

# 绘制热度图，调整颜色映射范围
plt.figure(figsize=(12, 8))
sns.heatmap(pivot_table, cmap='YlGnBu', cbar_kws={'label': '数量'}, vmin=data['总价'].min(), vmax=data['总价'].max())
plt.xlabel('房屋总价分箱')
plt.ylabel('区')
plt.title('各区二手房房屋总价分布热度图')
plt.show()
