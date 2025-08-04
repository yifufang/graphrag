#!/usr/bin/env python3
"""
清空Neo4j数据库脚本
用于在导入GraphRAG数据之前清空现有数据
"""

from neo4j import GraphDatabase
import sys

def clear_neo4j_database():
    """清空Neo4j数据库中的所有数据"""
    
    # 连接配置
    NEO4J_URI = "neo4j://localhost"
    NEO4J_USERNAME = "neo4j" 
    NEO4J_PASSWORD = "password"
    NEO4J_DATABASE = "neo4j"
    
    print("🗑️  Neo4j数据库清空工具")
    print("=" * 50)
    print("⚠️  警告：这将删除数据库中的所有数据！")
    print("⚠️  包括：所有节点、关系、索引和约束")
    print()
    
    # 确认操作
    confirm1 = input("确认要清空数据库吗？(yes/no): ").lower()
    if confirm1 != 'yes':
        print("❌ 操作已取消")
        return False
    
    confirm2 = input("再次确认，输入 'DELETE ALL' 来确认删除: ")
    if confirm2 != 'DELETE ALL':
        print("❌ 操作已取消")
        return False
    
    try:
        # 连接数据库
        print("\n🔌 连接到Neo4j...")
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
        
        with driver.session(database=NEO4J_DATABASE) as session:
            
            # 1. 检查数据库状态
            node_count = session.run("MATCH (n) RETURN count(n) as count").single()["count"]
            rel_count = session.run("MATCH ()-[r]->() RETURN count(r) as count").single()["count"]
            
            print(f"📊 当前数据库状态:")
            print(f"   节点数: {node_count:,}")
            print(f"   关系数: {rel_count:,}")
            
            if node_count == 0 and rel_count == 0:
                print("✅ 数据库已经为空")
                return True
            
            # 2. 删除所有数据
            print("\n🗑️  开始删除所有数据...")
            session.run("""
                MATCH (n)
                CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS
            """)
            print("✅ 所有节点和关系已删除")
            
            # 3. 删除约束
            print("\n🗑️  删除所有约束...")
            try:
                constraints_result = session.run("SHOW CONSTRAINTS")
                constraints = [record["name"] for record in constraints_result]
                
                for constraint_name in constraints:
                    try:
                        session.run(f"DROP CONSTRAINT {constraint_name}")
                        print(f"   ✅ 删除约束: {constraint_name}")
                    except Exception as e:
                        print(f"   ⚠️  无法删除约束 {constraint_name}: {e}")
                        
            except Exception as e:
                print(f"   ⚠️  获取约束列表失败: {e}")
            
            # 4. 删除索引
            print("\n🗑️  删除所有索引...")
            try:
                indexes_result = session.run("SHOW INDEXES")
                indexes = [(record["name"], record["type"]) for record in indexes_result]
                
                for index_name, index_type in indexes:
                    if index_type != 'LOOKUP':  # 保留系统索引
                        try:
                            session.run(f"DROP INDEX {index_name}")
                            print(f"   ✅ 删除索引: {index_name}")
                        except Exception as e:
                            print(f"   ⚠️  无法删除索引 {index_name}: {e}")
                            
            except Exception as e:
                print(f"   ⚠️  获取索引列表失败: {e}")
            
            # 5. 验证清空结果
            print("\n📊 验证清空结果...")
            final_node_count = session.run("MATCH (n) RETURN count(n) as count").single()["count"]
            final_rel_count = session.run("MATCH ()-[r]->() RETURN count(r) as count").single()["count"]
            
            print(f"   节点数: {final_node_count}")
            print(f"   关系数: {final_rel_count}")
            
            if final_node_count == 0 and final_rel_count == 0:
                print("\n🎉 数据库已完全清空!")
                print("✅ 现在可以安全导入GraphRAG数据了")
                return True
            else:
                print("\n⚠️  清空可能不完整，请检查数据库状态")
                return False
                
        driver.close()
        
    except Exception as e:
        print(f"\n❌ 清空数据库失败: {e}")
        print("\n💡 请检查:")
        print("   1. Neo4j服务是否正在运行")
        print("   2. 连接参数是否正确")
        print("   3. 用户权限是否足够")
        return False

def main():
    """主函数"""
    success = clear_neo4j_database()
    
    if success:
        print("\n🚀 下一步：运行Neo4j导入")
        print("   方法1: 在VS Code中打开 neo4j_import.ipynb")
        print("   方法2: 运行 jupyter lab neo4j_import.ipynb")
        print("   方法3: 运行 python neo4j_setup.py 检查环境")
    else:
        print("\n❌ 清空失败，请检查错误信息")
        sys.exit(1)

if __name__ == "__main__":
    main() 