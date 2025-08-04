#!/usr/bin/env python3
"""
分析GraphRAG中type为空字符串的实体产生原因
重点分析prompt设计的影响
"""

def analyze_prompt_issues():
    """分析当前prompt可能导致空type的问题"""
    print("🔍 Prompt导致空type实体的原因分析")
    print("="*60)
    
    print("📋 当前prompt的关键要求:")
    print("-" * 40)
    print("1. '识别所有实体'")
    print("2. 'entity_type: 以下类型之一：[{entity_types}]'")
    print("3. 示例中都正确分配了类型")
    print("4. 没有明确说明无法分类时怎么办")
    
    print(f"\n❌ Prompt设计的问题:")
    print("-" * 40)
    print("🎯 1. 指令冲突:")
    print("   - '识别所有实体' vs '只使用预定义类型'")
    print("   - 当遇到重要但不属于6类的实体时，LLM陷入困境")
    print("   - 结果：要么创造新类型，要么留空")
    
    print(f"\n🤖 2. LLM的处理策略:")
    print("   - 优先完成'识别所有实体'的任务")
    print("   - 当无法确定类型时，输出实体但留空type字段")
    print("   - 这解释了为什么有447个(11.7%)空type实体")
    
    print(f"\n📊 3. 中医术语的边界模糊:")
    print("   - 某些概念可能属于多个类型")
    print("   - 例如：'气虚血瘀'既是症状又是病机")
    print("   - LLM无法确定时选择留空")

def show_specific_examples():
    """展示可能导致空type的具体情况"""
    print(f"\n🔍 可能导致空type的具体情况:")
    print("="*60)
    
    examples = [
        {
            "entity": "阴阳失调",
            "reason": "可能被认为是病因、症状或中医基本概念，LLM难以分类"
        },
        {
            "entity": "君臣佐使",
            "reason": "配伍理论，不属于6个预定义类型中的任何一个"
        },
        {
            "entity": "望闻问切",
            "reason": "诊断方法，LLM创造了'诊断方法'类型，但也可能留空"
        },
        {
            "entity": "药性理论",
            "reason": "抽象概念，难以归入具体类型"
        },
        {
            "entity": "治未病",
            "reason": "预防医学理念，LLM可能不知道归入哪类"
        }
    ]
    
    for i, example in enumerate(examples, 1):
        print(f"{i}. 实体: '{example['entity']}'")
        print(f"   可能原因: {example['reason']}")
        print()

def analyze_graphrag_processing():
    """分析GraphRAG处理流程中的问题"""
    print(f"🔄 GraphRAG处理流程分析:")
    print("="*60)
    
    print("1. 📝 文本分块:")
    print("   - 长文档被分成多个chunk")
    print("   - 每个chunk独立提取实体")
    print("   - 上下文不完整可能导致分类困难")
    
    print(f"\n2. 🤖 多次LLM调用:")
    print("   - 不同chunk的相同实体可能被不同处理")
    print("   - 第一次调用分配了类型，后续调用可能留空")
    print("   - 累积效应导致数据不一致")
    
    print(f"\n3. 🔀 实体合并:")
    print("   - GraphRAG会合并重复实体")
    print("   - 如果同一实体在不同chunk有不同type（包括空值）")
    print("   - 最终可能保留空值")

def compare_with_data():
    """与实际数据对比分析"""
    print(f"\n📊 与实际数据对比:")
    print("="*60)
    
    print("观察到的现象:")
    print("✅ 预定义类型实体: 3,307 (86.3%) - 大部分正确")
    print("❌ 额外类型实体: 79 (2.1%) - LLM的'创造性'")
    print("⚠️  空类型实体: 447 (11.7%) - 这是我们分析的重点")
    
    print(f"\n💡 这个比例说明什么:")
    print("- 11.7%的空type比例相当高")
    print("- 说明prompt确实有指导不明确的问题") 
    print("- LLM在遇到难以分类的实体时倾向于留空")
    print("- 这比'创造新类型'(2.1%)更常见")

def show_solutions():
    """展示解决方案"""
    print(f"\n🛠️  针对空type问题的解决方案:")
    print("="*60)
    
    print("📋 方案1: 改进Prompt设计")
    print("```")
    print("添加明确的分类指导:")
    print("- 如果实体明确属于预定义类型，使用该类型")
    print("- 如果实体不属于任何预定义类型，使用'其他'类型")
    print("- 禁止留空type字段")
    print("```")
    
    print(f"\n🎯 方案2: 添加兜底类型")
    print("在settings.yaml中添加:")
    print("entity_types: [药材,方剂,疾病,症状,医家,功效,其他]")
    
    print(f"\n🔧 方案3: 后处理修复")
    print("```python")
    print("# 为空type实体分配默认类型")
    print("MATCH (n:Entity) WHERE n.type = ''")
    print("SET n.type = '未分类'")
    print("```")
    
    print(f"\n📝 方案4: 优化prompt示例")
    print("在prompt中添加处理边界情况的示例:")
    print("- 展示如何处理难以分类的实体")
    print("- 提供使用'其他'类型的例子")

def create_improved_prompt_suggestion():
    """创建改进的prompt建议"""
    print(f"\n📝 改进的Prompt建议:")
    print("="*60)
    
    print("在extract_graph.txt的第19行修改为:")
    print("```")
    print("- entity_type: 必须是以下类型之一：[{entity_types}]")
    print("  ⚠️ 重要说明：")
    print("  - 如果实体明确属于某个预定义类型，使用该类型")
    print("  - 如果实体重要但不属于任何预定义类型，使用'其他'类型")
    print("  - 绝对不要留空type字段")
    print("  - 绝对不要创造新的类型名称")
    print("```")
    
    print(f"\n并在settings.yaml中添加'其他'类型:")
    print("entity_types: [药材,方剂,疾病,症状,医家,功效,其他]")

def main():
    """主函数"""
    analyze_prompt_issues()
    show_specific_examples()
    analyze_graphrag_processing()
    compare_with_data()
    show_solutions()
    create_improved_prompt_suggestion()
    
    print(f"\n📋 结论:")
    print("="*60)
    print("🎯 是的，空type问题主要是prompt设计导致的：")
    print("")
    print("✅ 根本原因:")
    print("   1. Prompt要求'识别所有实体'但类型限制严格")
    print("   2. 没有明确说明无法分类时怎么办")
    print("   3. 缺少处理边界情况的指导")
    print("")
    print("📊 数据证据:")
    print("   - 11.7%的空type比例说明问题确实存在")
    print("   - 这比'创造新类型'(2.1%)更常见")
    print("   - 说明LLM倾向于留空而不是创造")
    print("")
    print("💡 解决建议:")
    print("   1. 添加'其他'类型作为兜底")
    print("   2. 强化prompt中的分类指导")
    print("   3. 提供处理边界情况的示例")
    print("   4. 后处理清理现有的空type数据")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ 分析失败: {e}")