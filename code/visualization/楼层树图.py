import pandas as pd
import plotly.express as px

# 读取CSV文件
file_path = r'C:\Users\11298\Desktop\combined_file.csv'
data = pd.read_csv(file_path)

# 统计各区域小区数量
area_counts = data['楼层'].value_counts().reset_index()

# 绘制树图
fig = px.treemap(area_counts, path=['index'], values='楼层', title='楼层数量分布')
# 显示图表
fig.show()