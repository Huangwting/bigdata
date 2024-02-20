import pandas as pd
import plotly.graph_objects as go
import seaborn as sns
import matplotlib.pyplot as plt

# 读取CSV文件
file_path = r'C:\Users\11298\Desktop\combined_file.csv'
data = pd.read_csv(file_path)
plt.rcParams['font.sans-serif'] = ['SimHei']

# 创建新字段 户型
data['户型'] = data['室'].astype(str) + data['厅'].astype(str) + data['卫'].astype(str)

# 删除包含NaN值的行
data = data.dropna(subset=['均价', '户型'])

# 统计各户型数量
bedroom_counts = data['户型'].value_counts()

# 绘制户型数量条形图
plt.figure(figsize=(12, 6))
bedroom_counts.plot(kind='bar', color='skyblue')
plt.xlabel('户型')
plt.ylabel('数量')
plt.title('各户型数量分布')
plt.show()

# 绘制户型对应的均价箱线图
plt.figure(figsize=(12, 6))
sns.boxplot(x='户型', y='均价', data=data, palette='Set3')
plt.xlabel('户型')
plt.ylabel('均价')
plt.title('各户型均价分布')
plt.xticks(rotation=45, ha='right')  # 使x轴标签倾斜，提高可读性
plt.show()

# 计算各种户型的数量
room_type_counts = data['户型'].value_counts()

# 创建漏斗图
fig = go.Figure()

# 添加户型字段的漏斗图
fig.add_trace(go.Funnel(
    name='户型',
    y=room_type_counts.index,
    x=room_type_counts.values,
    textinfo="value+percent initial"))

# 配置图表布局
fig.update_layout(title_text="房屋户型漏斗图", showlegend=True)

# 显示图形
fig.show()
