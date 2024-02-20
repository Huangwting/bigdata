from lib2to3.pgen2.pgen import DFAState
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# 读取CSV文件
file_path = r'C:\Users\11298\Desktop\combined_file.csv'
data = pd.read_csv(file_path)
plt.rcParams['font.sans-serif']=['SimHei']

# 删除包含NaN值的行
data = data.dropna(subset=['均价', '区'])

# 绘制天津市各区均价箱线图
plt.figure(figsize=(14, 8))
sns.boxplot(x='区', y='均价', data=data, palette='Set3')
plt.xlabel('区')
plt.ylabel('均价')
plt.title('天津市各区均价箱线图')
plt.xticks(rotation=45, ha='right')
plt.show()