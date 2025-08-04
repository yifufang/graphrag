#!/usr/bin/env python3
"""
分析Neo4j中type为空字符串的实体原因
基于对build_neo4j.py代码的分析
"""

import sys

def analyze_import_logic():
    """分析导入逻辑中可能产生空type的原因"""
    print("🔍 分析Neo4j导入逻辑中的type处理")
    print("="*60)
    
    print("📋 在build_neo4j.py第211行发现关键代码:")
    print("```python")
    print("entity_type = str(row.get('type', '')).strip().strip('\"') if pd.notna(row.get('type')) else ''")
    print("```")
    
    print(f"\n💡 这段代码会导致type为空字符串的情况:")
    print("="*60)
    
    print("1. 🔴 原始数据中type为NaN/NULL:")
    print("   - 如果entities.parquet中某行的type字段是NaN")
    print("   - pd.notna()返回False，直接设为空字符串''")
    print("   - 这是最可能的原因")
    
    print("\n2. 🟡 原始数据中type为空字符串或空格:")
    print("   - 如果原始type是'', '  ', '\"\"'等")
    print("   - 经过str().strip().strip('\"')处理后变成''")
    print("   - 比如GraphRAG提取时LLM返回了空值")
    
    print("\n3. 🟢 原始数据中type为引号包围的空值:")
    print("   - 如果原始type是'\"\"', '\" \"'等")
    print("   - strip('\"')会移除引号，最终变成空字符串")
    
    print(f"\n🎯 最可能的原因分析:")
    print("="*60)
    print("根据GraphRAG的工作原理，最可能的情况是:")
    print("1. 🤖 LLM在实体提取时没有为某些实体分配类型")
    print("2. 📊 这些实体的type字段在parquet文件中被存储为NaN")
    print("3. 🔄 导入Neo4j时，NaN被转换为空字符串")

def show_graphrag_config_analysis():
    """分析GraphRAG配置可能的影响"""
    print(f"\n🛠️  GraphRAG配置分析:")
    print("="*60)
    
    print("在settings.yaml中发现entity_types配置:")
    print("entity_types: [药材,方剂,疾病,症状,医家,功效]")
    
    print(f"\n💭 可能导致空type的GraphRAG原因:")
    print("1. 🎯 提取的实体不在预定义的6个类型中")
    print("2. 🤖 LLM模型在某些情况下无法确定实体类型") 
    print("3. 📝 输入文本中的某些实体表达模糊")
    print("4. 🔧 提取prompt可能需要优化")

def recommend_solutions():
    """推荐解决方案"""
    print(f"\n💡 解决方案建议:")
    print("="*60)
    
    print("🔍 1. 诊断原因:")
    print("   python -c \"import pandas as pd; df=pd.read_parquet('output/entities.parquet'); print('NaN count:', df['type'].isna().sum())\"")
    
    print("\n🛠️  2. GraphRAG层面优化:")
    print("   - 检查extract_graph.txt prompt是否明确要求为每个实体分配类型")
    print("   - 考虑在entity_types中添加'其他'类型作为兜底")
    print("   - 增加max_gleanings提高提取质量")
    
    print("\n🔧 3. 导入层面修复:")
    print("   - 修改build_neo4j.py，为空type实体分配默认类型")
    print("   - 例如：entity_type = entity_type or '未分类'")
    
    print("\n📊 4. 后处理清理:")
    print("   - 使用Neo4j查询分析这些实体的特征")
    print("   - 根据name或description手动分类")
    print("   - 或者考虑删除这些低质量实体")

def create_simple_neo4j_query():
    """创建简单的Neo4j查询来验证"""
    print(f"\n🔍 验证查询 (可在Neo4j浏览器中运行):")
    print("="*60)
    
    queries = [
        {
            "name": "统计空type实体",
            "query": "MATCH (n:Entity) WHERE n.type = '' RETURN count(n) as empty_type_count"
        },
        {
            "name": "查看空type实体样例",
            "query": "MATCH (n:Entity) WHERE n.type = '' RETURN n.name, n.description LIMIT 10"
        },
        {
            "name": "分析空type实体的描述特征", 
            "query": "MATCH (n:Entity) WHERE n.type = '' AND n.description IS NOT NULL RETURN substring(n.description, 0, 50) as desc_preview, count(*) as count ORDER BY count DESC LIMIT 10"
        },
        {
            "name": "查看所有实体类型分布",
            "query": "MATCH (n:Entity) RETURN n.type, count(n) as count ORDER BY count DESC"
        }
    ]
    
    for i, q in enumerate(queries, 1):
        print(f"{i}. {q['name']}:")
        print(f"   {q['query']}")
        print()

def main():
    """主函数"""
    print("🔍 Neo4j中type为空字符串的实体分析报告")
    print("="*60)
    
    # 分析导入逻辑
    analyze_import_logic()
    
    # 分析GraphRAG配置
    show_graphrag_config_analysis()
    
    # 推荐解决方案
    recommend_solutions()
    
    # 提供查询方法
    create_simple_neo4j_query()
    
    print(f"\n📋 总结:")
    print("="*60)
    print("type为空字符串的实体很可能是:")
    print("✅ GraphRAG原始输出中type字段为NaN的实体")
    print("✅ 在Neo4j导入时被转换为空字符串")
    print("✅ 这是数据源头问题，不是导入过程的bug")
    print("✅ 建议优化GraphRAG提取过程或后处理清理这些实体")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ 用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 分析过程出错: {e}")
        sys.exit(1)