#!/usr/bin/env python3
"""
检查GraphRAG原始输出中的type字段情况
确定空type是源头问题还是导入过程问题
"""

import pandas as pd
import os

def check_entities_parquet():
    """检查entities.parquet原始数据"""
    entities_file = "output/entities.parquet"
    
    if not os.path.exists(entities_file):
        print(f"❌ 文件不存在: {entities_file}")
        return
    
    print(f"📊 分析GraphRAG原始输出: {entities_file}")
    print("="*60)
    
    # 读取数据
    df = pd.read_parquet(entities_file)
    total_count = len(df)
    print(f"📈 总实体数量: {total_count:,}")
    
    # 检查type字段的各种情况
    print(f"\n🏷️  type字段详细分析:")
    print("-" * 40)
    
    # 1. NaN/NULL值
    nan_count = df['type'].isna().sum()
    print(f"type为NaN/NULL的数量: {nan_count:,} ({nan_count/total_count*100:.2f}%)")
    
    # 2. 空字符串
    empty_string_count = len(df[df['type'] == ''])
    print(f"type为空字符串('')的数量: {empty_string_count:,} ({empty_string_count/total_count*100:.2f}%)")
    
    # 3. 纯空格
    whitespace_count = len(df[df['type'].str.strip() == '']) - empty_string_count
    print(f"type为纯空格的数量: {whitespace_count:,} ({whitespace_count/total_count*100:.2f}%)")
    
    # 4. 有效值
    valid_count = len(df[df['type'].notna() & (df['type'].str.strip() != '')])
    print(f"type有有效值的数量: {valid_count:,} ({valid_count/total_count*100:.2f}%)")
    
    # 显示type值分布
    print(f"\n📊 原始数据中的type值分布:")
    print("-" * 50)
    type_counts = df['type'].value_counts(dropna=False)
    for i, (type_val, count) in enumerate(type_counts.head(15).items(), 1):
        if pd.isna(type_val):
            type_display = "⚠️ NaN/NULL (这些会导致Neo4j中的空字符串!)"
        elif type_val == '':
            type_display = "'' (空字符串)"
        elif type_val.strip() == '':
            type_display = f"'{type_val}' (纯空格)"
        else:
            type_display = f"'{type_val}'"
        print(f"{i:2d}. {type_display}: {count:,}")
    
    # 显示NaN实体的样例
    if nan_count > 0:
        print(f"\n🔍 NaN type实体样例 (这些在Neo4j中会变成空字符串):")
        print("-" * 70)
        nan_entities = df[df['type'].isna()].head(5)
        for i, (idx, row) in enumerate(nan_entities.iterrows(), 1):
            print(f"{i}. ID: {row.get('id', 'N/A')}")
            print(f"   名称: {row.get('title', 'N/A')}")
            desc = str(row.get('description', 'N/A'))
            print(f"   描述: {desc[:80]}...")
            print()
    
    return {
        'total': total_count,
        'nan_count': nan_count,
        'empty_string_count': empty_string_count,
        'valid_count': valid_count
    }

def explain_neo4j_conversion():
    """解释Neo4j转换过程"""
    print(f"\n🔄 Neo4j导入过程分析:")
    print("="*60)
    
    print("在build_neo4j.py第211行的处理逻辑:")
    print("```python")
    print("entity_type = str(row.get('type', '')).strip().strip('\"') if pd.notna(row.get('type')) else ''")
    print("```")
    
    print(f"\n💡 转换规则:")
    print("✅ 如果原始type是有效值 → 保持不变")
    print("⚠️  如果原始type是NaN/NULL → 转换为空字符串 ''")  
    print("⚠️  如果原始type是空字符串 → 保持空字符串 ''")
    print("⚠️  如果原始type是纯空格 → trim后变成空字符串 ''")
    
    print(f"\n🎯 结论:")
    print("Neo4j中type为空字符串的实体 = GraphRAG原始输出中type为NaN的实体")

def main():
    """主函数"""
    print("🔍 GraphRAG数据源头分析")
    print("确定空type的真正原因")
    print("="*60)
    
    # 检查原始数据
    stats = check_entities_parquet()
    
    if stats:
        # 解释转换过程
        explain_neo4j_conversion()
        
        # 给出最终结论
        print(f"\n📋 最终答案:")
        print("="*60)
        if stats['nan_count'] > 0:
            print(f"🎯 type为空字符串的原因: GraphRAG原始数据问题")
            print(f"📊 原始entities.parquet中有{stats['nan_count']:,}个实体的type字段为NaN")
            print(f"🔄 这些NaN在导入Neo4j时被转换为空字符串")
            print(f"")
            print(f"💡 这说明:")
            print(f"   - GraphRAG的LLM在提取这些实体时没有分配类型")
            print(f"   - 可能是实体类型不在预定义列表中")
            print(f"   - 或者LLM无法确定这些实体的类型")
        else:
            print(f"✅ 原始数据质量良好，所有实体都有type")
            print(f"🔍 需要进一步检查Neo4j导入过程")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ 分析失败: {e}")
        print("💡 请确保在GraphRAG虚拟环境中运行此脚本")