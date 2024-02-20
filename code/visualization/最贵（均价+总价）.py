import pandas as pd
import matplotlib.pyplot as plt

# 读取CSV文件
file_path = r'C:\Users\11298\Desktop\combined_file.csv'
data = pd.read_csv(file_path)
plt.rcParams['font.sans-serif'] = ['SimHei']

# 将均价和总价列转换为数值型
data['均价'] = pd.to_numeric(data['均价'], errors='coerce')
data['总价'] = pd.to_numeric(data['总价'], errors='coerce')

# 删除包含NaN值的行
data = data.dropna(subset=['均价', '总价'])

# 按均价和总价降序排序数据
sorted_data_by_avg_price = data.sort_values(by='均价', ascending=False)
sorted_data_by_total_price = data.sort_values(by='总价', ascending=False)

# 选择均价和总价最贵的前10个小区
top_n = 10
top_n_data_by_avg_price = sorted_data_by_avg_price.head(top_n)
top_n_data_by_total_price = sorted_data_by_total_price.head(top_n)

# 绘制均价最贵的10个小区的柱状图
plt.figure(figsize=(12, 8))
plt.barh(top_n_data_by_avg_price['道路'], top_n_data_by_avg_price['均价'], color='skyblue')
plt.xlabel('均价')
plt.ylabel('道路')
plt.title(f'均价最贵的{top_n}个小区')
plt.gca().invert_yaxis()  # 反转y轴，使均价最高的小区在顶部
plt.show()

# 绘制总价最贵的10个小区的柱状图
plt.figure(figsize=(12, 8))
plt.barh(top_n_data_by_total_price['道路'], top_n_data_by_total_price['总价'], color='salmon')
plt.xlabel('总价')
plt.ylabel('道路')
plt.title(f'总价最贵的{top_n}个小区')
plt.gca().invert_yaxis()  # 反转y轴，使总价最高的小区在顶部
plt.show()
