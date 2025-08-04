#!/usr/bin/env python3
"""
分析GraphRAG输出的entities.parquet文件
检查type字段的情况，判断空type是源头问题还是导入过程问题
"""

import subprocess
import sys
import os

def install_pandas():
    """安装pandas"""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "--quiet"])
        print("✅ pandas安装成功")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ pandas安装失败: {e}")
        return False

def analyze_entities_file():
    """分析entities.parquet文件"""
    import pandas as pd
    import numpy as np
    
    entities_file = "output/entities.parquet"
    
    if not os.path.exists(entities_file):
        print(f"❌ 文件不存在: {entities_file}")
        return
    
    print(f"📊 分析文件: {entities_file}")
    print("="*60)
    
    # 读取数据
    df = pd.read_parquet(entities_file)
    print(f"📈 总实体数量: {len(df):,}")
    
    # 检查列结构
    print(f"\n📋 数据列: {list(df.columns)}")
    
    # 分析type字段
    if 'type' in df.columns:
        print(f"\n🏷️  type字段分析:")
        print("-" * 40)
        
        # 统计各种情况
        total_count = len(df)
        empty_string_count = len(df[df['type'] == ''])
        nan_count = df['type'].isna().sum()
        null_count = len(df[df['type'].isnull()])
        valid_type_count = len(df[df['type'].notna() & (df['type'] != '')])
        
        print(f"总数量: {total_count:,}")
        print(f"type为空字符串('')的数量: {empty_string_count:,} ({empty_string_count/total_count*100:.2f}%)")
        print(f"type为NaN/NULL的数量: {nan_count:,} ({nan_count/total_count*100:.2f}%)")
        print(f"type有效值的数量: {valid_type_count:,} ({valid_type_count/total_count*100:.2f}%)")
        
        # 显示type值分布
        print(f"\n📊 type值分布 (Top 15):")
        print("-" * 40)
        type_counts = df['type'].value_counts(dropna=False)
        for i, (type_val, count) in enumerate(type_counts.head(15).items(), 1):
            if pd.isna(type_val):
                type_display = "NaN/NULL"
            elif type_val == '':
                type_display = "'' (空字符串) ⭐"
            else:
                type_display = f"'{type_val}'"
            print(f"{i:2d}. {type_display}: {count:,}")
        
        # 检查空type实体的详细信息
        if empty_string_count > 0:
            print(f"\n🔍 空type实体样例 (前5个):")
            print("-" * 60)
            empty_type_entities = df[df['type'] == ''].head(5)
            for i, (idx, row) in enumerate(empty_type_entities.iterrows(), 1):
                print(f"{i}. ID: {row.get('id', 'N/A')}")
                print(f"   标题: {row.get('title', 'N/A')}")
                print(f"   描述: {str(row.get('description', 'N/A'))[:100]}...")
                print(f"   其他字段: {[col for col in row.index if col not in ['id', 'title', 'description', 'type']]}")
                print()
    else:
        print("❌ 数据中没有'type'字段")
    
    # 检查是否有其他可能的类型字段
    type_like_columns = [col for col in df.columns if 'type' in col.lower()]
    if type_like_columns:
        print(f"\n🔍 包含'type'的字段: {type_like_columns}")

def analyze_neo4j_import_logic():
    """分析Neo4j导入逻辑"""
    print(f"\n🔧 分析Neo4j导入逻辑:")
    print("="*60)
    
    # 读取build_neo4j.py文件
    if os.path.exists("build_neo4j.py"):
        print("📁 检查build_neo4j.py中的type处理逻辑...")
        
        with open("build_neo4j.py", "r", encoding="utf-8") as f:
            content = f.read()
        
        # 查找type处理相关的代码
        import re
        
        # 查找处理type字段的代码行
        type_lines = []
        for i, line in enumerate(content.split('\n'), 1):
            if 'type' in line.lower() and ('row' in line or 'entity' in line):
                type_lines.append((i, line.strip()))
        
        if type_lines:
            print("🔍 发现type字段处理的相关代码:")
            for line_num, line in type_lines[:10]:  # 只显示前10行
                print(f"   Line {line_num}: {line}")
        
        # 查找可能导致空字符串的逻辑
        if "strip().strip('\"')" in content:
            print("\n⚠️  发现字符串清理逻辑: strip().strip('\"')")
            print("   这可能会将某些值转换为空字符串")
        
        if "if pd.notna(" in content:
            print("\n✅ 发现NaN检查逻辑")
        
    else:
        print("❌ build_neo4j.py文件不存在")

def main():
    """主函数"""
    print("🔍 GraphRAG entities.parquet 数据分析")
    print("分析type为空字符串的原因")
    print("="*60)
    
    # 检查并安装pandas
    try:
        import pandas as pd
        print("✅ pandas已安装")
    except ImportError:
        print("📦 正在安装pandas...")
        if not install_pandas():
            return 1
        import pandas as pd
    
    # 分析原始数据文件
    analyze_entities_file()
    
    # 分析导入逻辑
    analyze_neo4j_import_logic()
    
    # 结论和建议
    print(f"\n📋 分析结论:")
    print("="*60)
    print("根据以上分析，type为空字符串可能的原因:")
    print("1. 📊 原始数据问题: GraphRAG提取时某些实体没有被正确分类")
    print("2. 🔧 导入过程问题: 数据清理过程中产生了空字符串")
    print("3. 🏷️  配置问题: entity_types配置与实际提取的实体类型不匹配")
    print("4. 🤖 LLM问题: 语言模型在某些情况下没有为实体分配类型")
    
    print(f"\n💡 建议解决方案:")
    print("1. 检查GraphRAG的entity_types配置")
    print("2. 查看提取日志中的错误信息")
    print("3. 考虑为空type实体手动分配类型")
    print("4. 调整实体提取的prompt模板")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ 用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 分析过程出错: {e}")
        sys.exit(1)