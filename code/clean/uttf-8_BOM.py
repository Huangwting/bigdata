import os
import shutil

def remove_bom(input_path, output_path):
    # 读取文件
    with open(input_path, 'rb') as file:
        content = file.read()

    # 去除BOM
    if content.startswith(b'\xef\xbb\xbf'):
        content = content[3:]

    # 写入新文件
    with open(output_path, 'wb') as file:
        file.write(content)

def convert_folder(input_folder, output_folder):
    # 创建输出文件夹（如果不存在）
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # 遍历输入文件夹中的所有文件
    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            input_path = os.path.join(input_folder, filename)
            output_path = os.path.join(output_folder, f"utf8_{filename}")

            # 调用移除BOM的函数
            remove_bom(input_path, output_path)

# 示例用法
input_folder = r'D:\1A学习\大学\大数据\大作业\房产数据\张可欣\beike'
output_folder = r'D:\1A学习\大学\大数据\大作业\房产数据\张可欣\utf-8'
convert_folder(input_folder, output_folder)