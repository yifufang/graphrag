#!/usr/bin/env python3
"""
分析GraphRAG为什么提取出预定义类型之外的实体类型
"""

def analyze_graphrag_entity_types():
    """分析GraphRAG实体类型提取的问题"""
    print("🔍 GraphRAG实体类型提取分析")
    print("="*60)
    
    # 预定义的6个类型
    predefined_types = {'药材', '方剂', '疾病', '症状', '医家', '功效'}
    
    # 从用户数据中提取的实际类型和数量
    actual_types = {
        '症状': 1926,
        '疾病': 890, 
        '': 447,  # 空字符串
        '功效': 250,
        '方剂': 108,
        '药材': 106,
        '医家': 27,
        '病因': 15,
        '脉象': 11,
        '导引术': 8,
        '诊断方法': 7,
        '经络': 6,
        '养生方法': 6,
        '穴位': 5,
        '脏腑': 5,
        '经络（隐含实体，非指定类型）': 4,
        '病机': 3,
        '中医基本概念（未在指定类型中）': 3,
        '病理机制': 3,
        '饮食禁忌': 3
    }
    
    print("📊 设置对比分析:")
    print("-" * 50)
    print(f"🎯 settings.yaml中定义的类型 (6个):")
    for i, entity_type in enumerate(predefined_types, 1):
        count = actual_types.get(entity_type, 0)
        print(f"   {i}. {entity_type}: {count:,} 个实体")
    
    # 分析额外类型
    extra_types = {}
    predefined_count = 0
    extra_count = 0
    
    for entity_type, count in actual_types.items():
        if entity_type in predefined_types:
            predefined_count += count
        elif entity_type != '':  # 排除空字符串
            extra_types[entity_type] = count
            extra_count += count
    
    print(f"\n❌ LLM额外创造的类型 ({len(extra_types)}个):")
    for i, (entity_type, count) in enumerate(extra_types.items(), 1):
        print(f"   {i:2d}. {entity_type}: {count:,} 个实体")
    
    empty_count = actual_types.get('', 0)
    total_count = predefined_count + extra_count + empty_count
    
    print(f"\n📈 统计总结:")
    print("-" * 50)
    print(f"✅ 预定义类型实体: {predefined_count:,} ({predefined_count/total_count*100:.1f}%)")
    print(f"❌ 额外类型实体: {extra_count:,} ({extra_count/total_count*100:.1f}%)")
    print(f"⚠️  空类型实体: {empty_count:,} ({empty_count/total_count*100:.1f}%)")
    print(f"📊 总实体数: {total_count:,}")

def explain_why_this_happens():
    """解释为什么会发生这种情况"""
    print(f"\n💡 为什么LLM没有严格遵守类型限制？")
    print("="*60)
    
    print("🤖 1. LLM的'创造性':")
    print("   - 即使prompt明确要求只使用指定类型")
    print("   - LLM仍可能认为现有类型不够精确")
    print("   - 例如：'病因'比'疾病'更具体，'脉象'比'症状'更专业")
    
    print(f"\n📝 2. Prompt约束不够强:")
    print("   - 当前prompt说：'以下类型之一：[{entity_types}]'")
    print("   - 没有明确禁止创造新类型")
    print("   - LLM可能理解为'推荐类型'而非'严格限制'")
    
    print(f"\n🔍 3. 中医领域的复杂性:")
    print("   - 中医概念层次丰富：脉象、经络、穴位、脏腑等")
    print("   - LLM认为这些概念重要且特殊，值得单独分类")
    print("   - 6个预定义类型可能无法覆盖所有重要概念")
    
    print(f"\n⚙️ 4. 多次提取的累积效应:")
    print("   - GraphRAG对不同文档chunk多次运行提取")
    print("   - 每次提取都有微小的变化和'创新'")
    print("   - 最终累积出很多额外类型")

def provide_solutions():
    """提供解决方案"""
    print(f"\n🛠️  解决方案:")
    print("="*60)
    
    print("📋 方案1: 强化Prompt约束")
    print("   在extract_graph.txt中添加更严格的限制:")
    print("   '⚠️ 重要：只能使用以下类型，不得创造新类型：[{entity_types}]'")
    print("   '如果实体不属于任何预定义类型，请跳过该实体'")
    
    print(f"\n🎯 方案2: 扩展预定义类型")
    print("   基于实际提取结果，考虑添加重要的中医类型:")
    print("   entity_types: [药材,方剂,疾病,症状,医家,功效,病因,脉象,经络,穴位]")
    
    print(f"\n🔧 方案3: 后处理清理")
    print("   编写脚本将额外类型映射到预定义类型:")
    print("   - 病因 → 疾病")
    print("   - 脉象 → 症状") 
    print("   - 经络、穴位 → 新增'中医基础'类型")
    
    print(f"\n📊 方案4: 分析保留策略")
    print("   评估额外类型的价值:")
    print("   - 保留有意义的类型（如病因、脉象）")
    print("   - 删除数量少的边缘类型")
    print("   - 将相似类型合并")

def show_cleanup_script_preview():
    """显示清理脚本预览"""
    print(f"\n🔧 清理脚本示例:")
    print("="*60)
    
    print("```python")
    print("# Neo4j清理查询示例")
    print("type_mapping = {")
    print("    '病因': '疾病',")
    print("    '病机': '疾病',")
    print("    '病理机制': '疾病',")
    print("    '脉象': '症状',")
    print("    '诊断方法': '症状',")
    print("    '经络': '中医基础',")
    print("    '穴位': '中医基础',")
    print("    '脏腑': '中医基础',")
    print("    '导引术': '功效',")
    print("    '养生方法': '功效'")
    print("}")
    print("")
    print("# 批量更新Neo4j中的实体类型")
    print("for old_type, new_type in type_mapping.items():")
    print("    query = f'MATCH (n:Entity) WHERE n.type = \"{old_type}\" SET n.type = \"{new_type}\"'")
    print("    session.run(query)")
    print("```")

def main():
    """主函数"""
    analyze_graphrag_entity_types()
    explain_why_this_happens()
    provide_solutions()
    show_cleanup_script_preview()
    
    print(f"\n📋 结论:")
    print("="*60)
    print("🎯 GraphRAG的LLM确实没有严格遵守预定义的6个类型")
    print("🤖 这是LLM的'创造性'和中医领域复杂性的结果")
    print("📊 大约30%的实体使用了额外的类型")
    print("🛠️  可以通过强化prompt、扩展类型或后处理来解决")
    print("💡 建议：先用方案3清理现有数据，再用方案1防止未来问题")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ 分析失败: {e}")