import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt

# 读取CSV文件
file_path = r'C:\Users\11298\Desktop\combined_file.csv'
data = pd.read_csv(file_path)
plt.rcParams['font.sans-serif']=['SimHei']

# 将均价和建筑年份列转换为数值型
data['均价'] = pd.to_numeric(data['均价'], errors='coerce')
data['建筑年份'] = pd.to_numeric(data['建筑年份'], errors='coerce')

# 去除包含NaN值的行
data = data.dropna(subset=['均价', '建筑年份', '区'])

# 统计各区均价中位数和建筑年份的平均值
median_price_by_area_year = data.groupby(['区', '建筑年份']).agg({
    '均价': 'median',
}).reset_index()

# 绘制3D散点图
fig = px.scatter_3d(median_price_by_area_year, x='建筑年份', y='区', z='均价',
                    color='均价', size='均价',
                    hover_data=['区', '建筑年份', '均价'], opacity=0.7)
fig.update_layout(title='各个区建筑年份和均价分布3D散点图',
                  scene=dict(xaxis_title='建筑年份', yaxis_title='区名称', zaxis_title='均价'))
fig.show()