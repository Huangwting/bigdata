import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import folium
from folium.plugins import HeatMap

# 读取CSV文件
file_path = r'C:\Users\11298\Desktop\combined_file.csv'
data = pd.read_csv(file_path)
plt.rcParams['font.sans-serif'] = ['SimHei']

# 删除包含NaN值的行
data = data.dropna(subset=['经度', '纬度', '均价'])

# 创建地图对象
map_center = [39.084158, 117.200983]  # 天津市中心坐标
tianjin_map = folium.Map(location=map_center, zoom_start=12)

# 添加热力图层
heat_data = [[row['纬度'], row['经度'], row['均价']] for index, row in data.iterrows()]
HeatMap(heat_data, radius=15).add_to(tianjin_map)

# 保存地图为HTML文件
tianjin_map.save("tianjin_heatmap.html")