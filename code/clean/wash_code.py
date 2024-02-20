import pandas as pd
import os

# 获取当前文件夹路径
current_folder = os.getcwd()

# 获取当前文件夹中所有CSV文件的文件名
csv_files = [file for file in os.listdir(current_folder) if file.endswith('.csv')]

# 循环处理每个CSV文件
for file_path in csv_files:

    # 读取CSV文件
    df = pd.read_csv(file_path)

    # 删除单元格小于8的行
    df = df.dropna(axis=0,thresh=18)
    
    # 删除重复行
    df = df.drop_duplicates()

    # 将处理后的数据保存回原文件，使用'utf-8-sig'编码以保留原文件的编码格式
    try:
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
    except Exception as e:
        print(f"An error occurred while saving {file_path}: {e}")
