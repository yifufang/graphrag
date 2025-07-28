#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
书籍章节合并工具
将data文件夹下所有书籍的章节文件合并成一个大的文本文件
"""

import os
import glob
from pathlib import Path

def read_file_content(file_path):
    """
    安全读取文件内容，支持多种编码
    """
    encodings = ['utf-8', 'gbk', 'gb2312', 'utf-8-sig']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"读取文件 {file_path} 时出错: {e}")
            return None
    
    print(f"无法读取文件 {file_path}，所有编码都失败")
    return None

def combine_books_by_library(data_root="../data", output_dir="output"):
    """
    按库分别合并书籍章节，每个库生成一个文件
    """
    if not os.path.exists(data_root):
        print(f"数据文件夹 {data_root} 不存在")
        return
    
    # 创建输出目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 支持的文件扩展名
    text_extensions = ['.txt', '.md']
    
    total_files = 0
    successful_files = 0
    libraries_processed = 0
    
    print("开始按库合并书籍章节...")
    
    # 遍历data下的每个库
    for library_name in os.listdir(data_root):
        library_path = os.path.join(data_root, library_name)
        if not os.path.isdir(library_path):
            continue
            
        print(f"\n处理库: {library_name}")
        libraries_processed += 1
        
        # 为每个库遍历其子目录
        for sub_library in os.listdir(library_path):
            sub_library_path = os.path.join(library_path, sub_library)
            if not os.path.isdir(sub_library_path):
                continue
                
            print(f"  处理子库: {sub_library}")
            
            combined_content = []
            library_files = 0
            library_successful = 0
            
            # 添加库的标题
            combined_content.append("="*100 + "\n")
            combined_content.append(f"【{sub_library}】\n")
            combined_content.append("="*100 + "\n\n")
            
            # 遍历子库下的所有书籍/方剂
            for root, dirs, files in os.walk(sub_library_path):
                # 跳过子库根目录
                if root == sub_library_path:
                    continue
                    
                # 检查当前目录是否包含文本文件
                text_files = []
                for file in files:
                    if any(file.lower().endswith(ext) for ext in text_extensions):
                        text_files.append(file)
                
                if text_files:
                    # 获取书籍/方剂名称
                    book_name = os.path.basename(root)
                    
                    # 添加书籍标题
                    combined_content.append("\n" + "-"*80 + "\n")
                    combined_content.append(f"【{book_name}】\n")
                    combined_content.append("-"*80 + "\n\n")
                    
                    print(f"    处理书籍: {book_name}")
                    
                    # 排序文件名，确保输出有序
                    text_files.sort()
                    
                    for file in text_files:
                        file_path = os.path.join(root, file)
                        library_files += 1
                        total_files += 1
                        
                        print(f"      处理文件: {file}")
                        
                        # 添加文件标题
                        combined_content.append(f"\n*** {file} ***\n\n")
                        
                        # 读取文件内容
                        content = read_file_content(file_path)
                        if content is not None:
                            combined_content.append(content)
                            combined_content.append("\n\n")
                            library_successful += 1
                            successful_files += 1
                        else:
                            combined_content.append("[文件读取失败]\n\n")
            
            # 只有当有内容时才写入文件
            if combined_content and library_files > 0:
                # 生成安全的文件名
                safe_filename = "".join(c for c in sub_library if c.isalnum() or c in (' ', '-', '_')).rstrip()
                output_filename = f"{safe_filename}.txt"
                output_path = os.path.join(output_dir, output_filename)
                
                try:
                    with open(output_path, 'w', encoding='utf-8') as f:
                        f.write(''.join(combined_content))
                    
                    print(f"    -> 生成文件: {output_filename}")
                    print(f"       包含 {library_files} 个章节文件")
                    print(f"       文件大小: {os.path.getsize(output_path) / 1024:.2f} KB")
                    
                except Exception as e:
                    print(f"    写入文件 {output_filename} 时出错: {e}")
    
    print(f"\n" + "="*50)
    print(f"合并完成！")
    print(f"处理的库数量: {libraries_processed}")
    print(f"总文件数: {total_files}")
    print(f"成功处理: {successful_files}")
    print(f"输出目录: {output_dir}")

def combine_books(data_root="../data", output_file="combined_books.txt"):
    """
    合并所有书籍章节到一个文件 (保留原函数)
    """
    if not os.path.exists(data_root):
        print(f"数据文件夹 {data_root} 不存在")
        return
    
    # 支持的文件扩展名
    text_extensions = ['.txt', '.md']
    
    combined_content = []
    total_files = 0
    successful_files = 0
    
    print("开始合并书籍章节...")
    
    # 遍历data文件夹下的所有内容
    for root, dirs, files in os.walk(data_root):
        # 跳过根目录
        if root == data_root:
            continue
            
        # 检查当前目录是否包含文本文件
        text_files = []
        for file in files:
            if any(file.lower().endswith(ext) for ext in text_extensions):
                text_files.append(file)
        
        if text_files:
            # 获取相对路径作为标识
            relative_path = os.path.relpath(root, data_root)
            path_parts = relative_path.split(os.sep)
            
            # 添加目录信息作为章节标题
            combined_content.append("\n" + "="*80 + "\n")
            combined_content.append(f"【{' > '.join(path_parts)}】\n")
            combined_content.append("="*80 + "\n\n")
            
            print(f"处理目录: {relative_path}")
            
            # 排序文件名，确保输出有序
            text_files.sort()
            
            for file in text_files:
                file_path = os.path.join(root, file)
                total_files += 1
                
                print(f"  处理文件: {file}")
                
                # 添加文件标题
                combined_content.append(f"\n--- {file} ---\n\n")
                
                # 读取文件内容
                content = read_file_content(file_path)
                if content is not None:
                    combined_content.append(content)
                    combined_content.append("\n\n")
                    successful_files += 1
                else:
                    combined_content.append("[文件读取失败]\n\n")
    
    # 写入合并后的文件
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(''.join(combined_content))
        
        print(f"\n合并完成！")
        print(f"总文件数: {total_files}")
        print(f"成功处理: {successful_files}")
        print(f"输出文件: {output_file}")
        print(f"文件大小: {os.path.getsize(output_file) / 1024 / 1024:.2f} MB")
        
    except Exception as e:
        print(f"写入输出文件时出错: {e}")

def get_statistics(data_root="data"):
    """
    获取数据统计信息
    """
    if not os.path.exists(data_root):
        print(f"数据文件夹 {data_root} 不存在")
        return
    
    text_extensions = ['.txt', '.md']
    stats = {
        'libraries': 0,
        'books': 0,
        'chapters': 0,
        'total_size': 0
    }
    
    print("数据统计:")
    
    for item in os.listdir(data_root):
        item_path = os.path.join(data_root, item)
        if os.path.isdir(item_path):
            print(f"\n库: {item}")
            
            library_books = 0
            library_chapters = 0
            
            for root, dirs, files in os.walk(item_path):
                if root == item_path:
                    continue
                    
                # 检查是否包含文本文件
                text_files = [f for f in files if any(f.lower().endswith(ext) for ext in text_extensions)]
                if text_files:
                    library_books += 1
                    library_chapters += len(text_files)
                    
                    # 计算文件大小
                    for file in text_files:
                        file_path = os.path.join(root, file)
                        try:
                            stats['total_size'] += os.path.getsize(file_path)
                        except:
                            pass
            
            print(f"  书籍/方剂数: {library_books}")
            print(f"  章节文件数: {library_chapters}")
            
            stats['libraries'] += 1
            stats['books'] += library_books
            stats['chapters'] += library_chapters
    
    print(f"\n总统计:")
    print(f"库的数量: {stats['libraries']}")
    print(f"书籍/方剂总数: {stats['books']}")
    print(f"章节文件总数: {stats['chapters']}")
    print(f"总文件大小: {stats['total_size'] / 1024 / 1024:.2f} MB")

if __name__ == "__main__":
    # 首先显示统计信息
    get_statistics()
    
    print("\n" + "="*50)
    
    # 按库分别合并书籍
    combine_books_by_library()
