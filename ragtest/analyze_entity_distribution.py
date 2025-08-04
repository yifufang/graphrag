#!/usr/bin/env python3
"""
分析实体类型分布
"""

try:
    import pandas as pd
    import numpy as np
    
    # 尝试不同的文件名
    possible_files = [
        './output/entities.parquet',
        './output/entities.parquet'
    ]
    
    df = None
    for file_path in possible_files:
        try:
            df = pd.read_parquet(file_path)
            print(f"✅ 成功读取: {file_path}")
            break
        except Exception as e:
            print(f"❌ 无法读取 {file_path}: {e}")
    
    if df is None:
        print("❌ 无法读取实体文件")
        exit(1)
    
    print(f"\n📊 实体数据统计:")
    print("="*50)
    print(f"总实体数: {len(df):,}")
    
    if 'type' in df.columns:
        print(f"\n🏷️ 实体类型分布:")
        print("-" * 30)
        
        type_counts = df['type'].value_counts()
        total = len(df)
        
        for entity_type, count in type_counts.head(20).items():
            percentage = count / total * 100
            if entity_type == '':
                print(f"{'[空类型]':<15}: {count:>6,} ({percentage:>5.1f}%)")
            else:
                print(f"{entity_type:<15}: {count:>6,} ({percentage:>5.1f}%)")
        
        # 统计问题实体
        empty_count = len(df[df['type'] == ''])
        predefined_types = ['药材','方剂','疾病','症状','医家','功效','病因','脉象','诊断方法','经络','穴位','脏腑']
        predefined_count = len(df[df['type'].isin(predefined_types)])
        other_count = total - empty_count - predefined_count
        
        print(f"\n📈 分类统计:")
        print("-" * 30)
        print(f"预定义类型实体: {predefined_count:,} ({predefined_count/total*100:.1f}%)")
        print(f"其他类型实体:   {other_count:,} ({other_count/total*100:.1f}%)")
        print(f"空类型实体:     {empty_count:,} ({empty_count/total*100:.1f}%)")
        
        if other_count > 0:
            print(f"\n🔍 非预定义类型:")
            print("-" * 30)
            other_types = df[~df['type'].isin(predefined_types + [''])]['type'].value_counts()
            for otype, count in other_types.head(10).items():
                print(f"{otype:<15}: {count:>6,}")
                
    else:
        print("❌ 没有找到type列")
        
except ImportError:
    print("❌ 需要安装pandas: pip install pandas")
except Exception as e:
    print(f"❌ 分析失败: {e}")