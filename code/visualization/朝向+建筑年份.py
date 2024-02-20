import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 读取CSV文件
file_path = r'C:\Users\11298\Desktop\combined_file.csv'
data = pd.read_csv(file_path)
plt.rcParams['font.sans-serif']=['SimHei']


# 统计朝向数量
orientation_counts = data['朝向'].value_counts()

# 绘制饼图
plt.figure(figsize=(8, 8))
plt.pie(orientation_counts, labels=orientation_counts.index, autopct='%1.1f%%', startangle=140, colors=plt.cm.Set3.colors)
plt.title('朝向分布')
plt.show()

# 绘制折线图
plt.figure(figsize=(12, 6))
sns.lineplot(x='建筑年份', y='数量', data=data.groupby('建筑年份').size().reset_index(name='数量'), marker='o', color='skyblue')
plt.xlabel('建筑年份')
plt.ylabel('数量')
plt.title('建筑年份分布趋势')
plt.show()