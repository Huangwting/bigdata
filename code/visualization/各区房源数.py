import pandas as pd
import matplotlib.pyplot as plt
from pyecharts.render import display
from pyecharts.globals import CurrentConfig, NotebookType
from pyecharts import options as opts
from pyecharts.charts import Geo, Map
from IPython.display import display

# 读取CSV文件
file_path = r'C:\Users\11298\Desktop\combined_file.csv'
data = pd.read_csv(file_path)
plt.rcParams['font.sans-serif'] = ['SimHei']

# 各地区二手房源数
tianjin = data.groupby('区').size().reset_index(name='待售房屋').sort_values('待售房屋', ascending=False).reset_index(drop=True)
tianjin.loc[~tianjin['区'].str.endswith('区'), '区'] = tianjin['区'] + '区'

# 使用pyecharts绘制地图
c = (
    Map(init_opts=opts.InitOpts(theme='dark', width='800px'))
    .add("房源数", [list(z) for z in zip(tianjin['区'].to_list(), tianjin['待售房屋'].to_list())], "天津", label_opts=opts.LabelOpts(is_show=False))
    .set_global_opts(
        visualmap_opts=opts.VisualMapOpts(max_=15000)
    )
)

# 在地图上标注每个区的名称
geo = Geo().add_schema(maptype="天津")
for index, row in tianjin.iterrows():
    geo.add_coordinate(row['区'], 0, 0)  # 为每个区域添加经纬度坐标，根据实际情况填写
    geo.add(
        series_name="区名",
        data_pair=[(row['区'], row['待售房屋'])],
        type_='scatter',
        symbol_size=0,
        symbol="pin",
        label_opts=opts.LabelOpts(
            is_show=True,
            position="top",
            formatter="{b}",
            font_size=12,
            color="black"
        ),
    )

# 展示图形
c.render('output_map_with_labels.html')
