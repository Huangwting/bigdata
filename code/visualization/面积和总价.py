import pandas as pd
import matplotlib.pyplot as plt

# 读取CSV文件
file_path = r'C:\Users\11298\Desktop\combined_file.csv'
data = pd.read_csv(file_path)
plt.rcParams['font.sans-serif']=['SimHei']

# 将面积和总价列转换为数值型
data['面积'] = pd.to_numeric(data['面积'], errors='coerce')
data['总价'] = pd.to_numeric(data['总价'], errors='coerce')

# 删除包含NaN值的行
data = data.dropna(subset=['面积', '总价'])

# 绘制散点图
plt.figure(figsize=(10, 6))
plt.scatter(data['面积'], data['总价'], alpha=0.5)
plt.title('面积和总价的散点图')
plt.xlabel('面积')
plt.ylabel('总价')
plt.grid(True)
plt.show()
