#!/usr/bin/env python3
"""
使用neomodel查询Neo4j中医知识图谱
查询type为空字符串的实体数量
"""

from neomodel import config, StructuredNode, StringProperty, IntegerProperty, FloatProperty, db
import sys

# 定义实体模型
class Entity(StructuredNode):
    """实体节点模型"""
    entity_id = StringProperty(unique_index=True, required=True)  # 避免与neomodel内部id冲突
    name = StringProperty(index=True)
    type = StringProperty(index=True)
    description = StringProperty()
    human_readable_id = IntegerProperty()
    degree = IntegerProperty()
    color = StringProperty()
    
    class Meta:
        app_label = "tcm_knowledge_graph"

def setup_database_connection():
    """设置数据库连接"""
    # 根据你的Neo4j配置设置连接
    NEO4J_URL = "bolt://neo4j:password@localhost:7687"
    config.DATABASE_URL = NEO4J_URL
    print("🔌 连接到Neo4j数据库...")
    
    try:
        # 测试连接
        db.cypher_query("RETURN 'Hello Neo4j' as message")
        print("✅ 数据库连接成功!")
        return True
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        print("💡 请确保:")
        print("   1. Neo4j服务正在运行")
        print("   2. 用户名密码正确: neo4j/password")
        print("   3. 数据库中已有实体数据")
        return False

def get_database_stats():
    """获取数据库基本统计信息"""
    print("\n📊 获取数据库统计信息...")
    
    try:
        # 总实体数量
        query = "MATCH (n:Entity) RETURN count(n) as total_entities"
        result, meta = db.cypher_query(query)
        total_entities = result[0][0] if result else 0
        
        # 有type的实体数量
        query = "MATCH (n:Entity) WHERE n.type IS NOT NULL AND n.type <> '' RETURN count(n) as entities_with_type"
        result, meta = db.cypher_query(query)
        entities_with_type = result[0][0] if result else 0
        
        # type为空字符串的实体数量  
        query = "MATCH (n:Entity) WHERE n.type = '' RETURN count(n) as empty_type_entities"
        result, meta = db.cypher_query(query)
        empty_type_entities = result[0][0] if result else 0
        
        # type为NULL的实体数量
        query = "MATCH (n:Entity) WHERE n.type IS NULL RETURN count(n) as null_type_entities"
        result, meta = db.cypher_query(query)
        null_type_entities = result[0][0] if result else 0
        
        print(f"📈 数据库统计:")
        print(f"   总实体数量: {total_entities:,}")
        print(f"   有type的实体: {entities_with_type:,}")
        print(f"   type为空字符串('')的实体: {empty_type_entities:,}")
        print(f"   type为NULL的实体: {null_type_entities:,}")
        
        return {
            'total': total_entities,
            'with_type': entities_with_type,
            'empty_type': empty_type_entities,
            'null_type': null_type_entities
        }
        
    except Exception as e:
        print(f"❌ 获取统计信息失败: {e}")
        return None

def query_empty_type_entities(limit=10):
    """查询type为空字符串的实体详情"""
    print(f"\n🔍 查询type为空字符串的实体 (显示前{limit}个)...")
    
    try:
        # 使用Cypher查询
        query = """
        MATCH (n:Entity) 
        WHERE n.type = '' 
        RETURN id(n), n.name, n.type, n.description 
        LIMIT $limit
        """
        result, meta = db.cypher_query(query, {'limit': limit})
        
        if result:
            print("📋 找到的实体:")
            print("-" * 80)
            for i, row in enumerate(result, 1):
                entity_id, name, entity_type, description = row
                desc_preview = (description[:50] + "...") if description and len(description) > 50 else (description or "无描述")
                print(f"{i:2d}. ID: {entity_id}")
                print(f"    名称: {name or '无名称'}")
                print(f"    类型: '{entity_type}' (空字符串)")
                print(f"    描述: {desc_preview}")
                print()
        else:
            print("✨ 没有找到type为空字符串的实体")
            
    except Exception as e:
        print(f"❌ 查询实体详情失败: {e}")

def query_type_distribution():
    """查询实体类型分布"""
    print("\n📊 查询实体类型分布...")
    
    try:
        query = """
        MATCH (n:Entity) 
        RETURN n.type as type, count(n) as count 
        ORDER BY count DESC 
        LIMIT 20
        """
        result, meta = db.cypher_query(query)
        
        if result:
            print("🏷️  实体类型分布 (Top 20):")
            print("-" * 50)
            for i, (entity_type, count) in enumerate(result, 1):
                type_display = f"'{entity_type}'" if entity_type == '' else (entity_type or 'NULL')
                if entity_type == '':
                    type_display += " (空字符串) ⭐"
                elif entity_type is None:
                    type_display += " (NULL值)"
                print(f"{i:2d}. {type_display}: {count:,}")
                
    except Exception as e:
        print(f"❌ 查询类型分布失败: {e}")

def main():
    """主函数"""
    print("🏥 中医知识图谱 - neomodel查询工具")
    print("="*60)
    
    # 1. 连接数据库
    if not setup_database_connection():
        return 1
    
    # 2. 获取数据库统计信息
    stats = get_database_stats()
    if stats is None:
        return 1
    
    # 3. 查询类型分布
    query_type_distribution()
    
    # 4. 查询type为空字符串的实体详情
    if stats['empty_type'] > 0:
        query_empty_type_entities(limit=10)
    
    # 5. 总结
    print("\n" + "="*60)
    print("📋 查询结果总结:")
    print("="*60)
    print(f"🎯 type为空字符串('')的实体数量: {stats['empty_type']:,}")
    
    if stats['empty_type'] > 0:
        percentage = (stats['empty_type'] / stats['total']) * 100 if stats['total'] > 0 else 0
        print(f"📊 占总实体的比例: {percentage:.2f}%")
        
        print(f"\n💡 建议:")
        print(f"   - 检查数据源中的实体类型字段")
        print(f"   - 考虑为这些实体分配合适的类型")
        print(f"   - 或者清理这些无类型的实体")
    else:
        print("✨ 所有实体都有有效的类型，数据质量良好!")
    
    return 0

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n❌ 用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 程序执行出错: {e}")
        sys.exit(1)