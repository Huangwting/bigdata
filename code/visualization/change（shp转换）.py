import fiona
import pandas as pd
from shapely.geometry import Point
 
# Shapefile文件路径
shapefile_path = '天津餐饮店.shp'
 
# 读取Shapefile
with fiona.open(shapefile_path, 'r') as shapefile:
    # 创建一个空的DataFrame
    df = pd.DataFrame()
 
    # 遍历每个要素
    for feature in shapefile:
        # 将GeoJSON转换为Shapely对象
        geometry = Point(feature['geometry']['coordinates'])
 
        # 将要素属性添加到DataFrame
        df_temp = pd.DataFrame([feature['properties']])
        df = pd.concat([df, df_temp], ignore_index=True)
 
    # 将Geometry列添加到DataFrame
    df['geometry'] = geometry
 
# 将DataFrame保存为CSV
df.to_csv('output.csv', index=False)