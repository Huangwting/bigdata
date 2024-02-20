import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud

# 读取CSV文件
file_path = 'c:\\Users\\11298\\Desktop\\zufang_wc.csv'
data = pd.read_csv(file_path, encoding='gbk')
plt.rcParams['font.sans-serif']=['SimHei']

# 将词频数据提取出来，这里假设词频数据在'词语'列中
word_frequencies = data['词语'].value_counts().to_dict()

font_path = 'c:\\Users\\11298\\Downloads\\simhei.ttf'

# 创建WordCloud对象
wordcloud = WordCloud(font_path='simhei.ttf', background_color='white', max_words=200).generate_from_frequencies(word_frequencies)

# 绘制词云图
plt.figure(figsize=(20, 10))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')  # 关闭坐标轴
plt.show()
