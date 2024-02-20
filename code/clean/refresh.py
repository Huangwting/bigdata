import pandas as pd
import os

# 指定CSV文件所在目录
input_directory = r'D:\1A学习\大学\大数据\大作业\房产数据\张可欣\utf-8'
output_directory = r'D:\1A学习\大学\大数据\大作业\房产数据\张可欣\output'

# 确保输出目录存在
os.makedirs(output_directory, exist_ok=True)

# 列出指定目录下的所有CSV文件
csv_files = [f for f in os.listdir(input_directory) if f.endswith('.csv')]

# 处理每个CSV文件
for csv_file in csv_files:
    # 构建完整的文件路径
    input_path = os.path.join(input_directory, csv_file)
    output_path = os.path.join(output_directory, f"{os.path.splitext(csv_file)[0]}_utf8.csv")

    # 使用pandas的read_csv方法处理CSV文件
    df = pd.read_csv(input_path)

    # 处理缺失数据，删除包含缺失值的行
    df = df.dropna()

    # 处理重复数据，删除重复行
    df = df.drop_duplicates()

    # 导出处理后的数据到新的CSV文件，指定编码方式为'utf-8-sig'，避免写入BOM头
    df.to_csv(output_path, index=False, encoding='utf-8-sig')

print("Conversion completed.")