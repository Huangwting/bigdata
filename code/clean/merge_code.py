import os
import pandas as pd

# 获取当前工作目录
current_directory = os.getcwd()

# 获取当前工作目录中所有CSV文件的路径
csv_files = [file for file in os.listdir(current_directory) if file.endswith('.csv')]

# 创建一个空的DataFrame来存储所有CSV文件的数据
combined_df = pd.DataFrame()

# 逐个读取CSV文件并合并
for file in csv_files:
    file_path = os.path.join(current_directory, file)
    df = pd.read_csv(file_path)
    combined_df = pd.concat([combined_df, df], ignore_index=True)

# 将合并后的DataFrame保存为新的CSV文件
combined_df.to_csv('combined_file.csv', index=False, encoding='utf-8-sig')
