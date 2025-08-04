#!/usr/bin/env python3
"""
数据库清理工具
删除Neo4j中type为空字符串的实体
"""

import sys
from neo4j import GraphDatabase
from typing import Optional
import warnings
warnings.filterwarnings('ignore')

class DatabaseCleaner:
    """数据库清理器 - 删除type为空的实体"""
    
    def __init__(self, uri: str = "neo4j://localhost", username: str = "neo4j", password: str = "password"):
        """初始化Neo4j连接"""
        self.uri = uri
        self.username = username
        self.password = password
        self.driver = None
    
    def connect(self) -> bool:
        """连接到Neo4j数据库"""
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
            # 测试连接
            with self.driver.session() as session:
                session.run("RETURN 1")
            print(f"✅ 成功连接到Neo4j数据库: {self.uri}")
            return True
        except Exception as e:
            print(f"❌ 连接Neo4j失败: {e}")
            print("请确保Neo4j服务正在运行，并检查连接参数")
            return False
    
    def close(self):
        """关闭数据库连接"""
        if self.driver:
            self.driver.close()
            print("🔌 数据库连接已关闭")
    
    def analyze_empty_types(self) -> dict:
        """分析type为空的实体情况"""
        print("\n🔍 分析type为空的实体...")
        print("="*60)
        
        if not self.driver:
            print("❌ 数据库连接未建立")
            return {}
        
        try:
            with self.driver.session() as session:
                # 统计总实体数
                total_result = session.run("MATCH (e:Entity) RETURN count(e) as total")
                total_entities = total_result.single()["total"]
                
                # 统计type为空的实体数
                empty_type_result = session.run("""
                    MATCH (e:Entity) 
                    WHERE e.type = '' OR e.type IS NULL
                    RETURN count(e) as empty_count
                """)
                empty_count = empty_type_result.single()["empty_count"]
                
                # 获取一些示例
                sample_result = session.run("""
                    MATCH (e:Entity) 
                    WHERE e.type = '' OR e.type IS NULL
                    RETURN e.id, e.name, e.type, e.description
                    LIMIT 5
                """)
                samples = [record.data() for record in sample_result]
                
                analysis = {
                    "total_entities": total_entities,
                    "empty_type_count": empty_count,
                    "percentage": (empty_count / total_entities * 100) if total_entities > 0 else 0,
                    "samples": samples
                }
                
                # 显示分析结果
                print(f"📊 总实体数量: {total_entities:,}")
                print(f"🔴 type为空的实体数量: {empty_count:,}")
                print(f"📈 空type实体占比: {analysis['percentage']:.2f}%")
                
                if samples:
                    print(f"\n📝 空type实体示例:")
                    for i, sample in enumerate(samples, 1):
                        print(f"   {i}. ID: {sample['e.id']}")
                        print(f"      名称: {sample['e.name']}")
                        print(f"      类型: '{sample['e.type']}'")
                        print(f"      描述: {sample['e.description'][:100]}..." if len(sample['e.description']) > 100 else f"      描述: {sample['e.description']}")
                        print()
                
                return analysis
                
        except Exception as e:
            print(f"❌ 分析失败: {e}")
            return {}
    
    def delete_empty_type_entities(self, confirm: bool = False) -> bool:
        """删除type为空的实体"""
        if not self.driver:
            print("❌ 数据库连接未建立")
            return False
        
        try:
            # 首先分析要删除的实体
            analysis = self.analyze_empty_types()
            empty_count = analysis.get("empty_type_count", 0)
            
            if empty_count == 0:
                print("✅ 没有发现type为空的实体，无需清理")
                return True
            
            # 确认删除
            if not confirm:
                print(f"\n⚠️  警告: 即将删除 {empty_count} 个type为空的实体!")
                response = input("确认删除? (y/N): ").strip().lower()
                if response not in ['y', 'yes', '是']:
                    print("❌ 操作已取消")
                    return False
            
            print(f"\n🗑️  开始删除type为空的实体...")
            
            with self.driver.session() as session:
                # 执行删除操作
                result = session.run("""
                    MATCH (e:Entity) 
                    WHERE e.type = '' OR e.type IS NULL
                    DETACH DELETE e
                    RETURN count(e) as deleted_count
                """)
                
                # 注意：DETACH DELETE 会先删除关系再删除节点
                # 但由于Cypher的特性，这里的count可能不准确
                # 我们需要用另一种方式统计
                
                # 重新统计确认删除结果
                remaining_result = session.run("""
                    MATCH (e:Entity) 
                    WHERE e.type = '' OR e.type IS NULL
                    RETURN count(e) as remaining_count
                """)
                remaining_count = remaining_result.single()["remaining_count"]
                
                deleted_count = empty_count - remaining_count
                
                print(f"✅ 删除完成!")
                print(f"   删除实体数量: {deleted_count}")
                print(f"   剩余空type实体: {remaining_count}")
                
                if remaining_count > 0:
                    print(f"⚠️  仍有 {remaining_count} 个空type实体未删除，可能存在并发访问或其他问题")
                
                return remaining_count == 0
                
        except Exception as e:
            print(f"❌ 删除操作失败: {e}")
            return False
    
    def verify_cleanup(self):
        """验证清理结果"""
        print(f"\n🔍 验证清理结果...")
        analysis = self.analyze_empty_types()
        
        empty_count = analysis.get("empty_type_count", 0)
        total_count = analysis.get("total_entities", 0)
        
        if empty_count == 0:
            print("✅ 清理完成! 数据库中已无type为空的实体")
        else:
            print(f"⚠️  仍有 {empty_count} 个type为空的实体")
        
        print(f"📊 当前数据库总实体数: {total_count:,}")

def main():
    """主函数"""
    print("🧹 数据库清理工具 - 删除type为空的实体")
    print("="*60)
    
    # 可以通过命令行参数自定义连接参数
    uri = "neo4j://localhost"
    username = "neo4j"
    password = "password"
    
    # 检查命令行参数
    if len(sys.argv) > 1:
        if sys.argv[1] in ['-h', '--help']:
            print("使用方法:")
            print("  python database_cleaning.py [--confirm]")
            print("  --confirm : 跳过确认提示直接删除")
            print("\n连接参数:")
            print(f"  URI: {uri}")
            print(f"  用户名: {username}")
            print(f"  密码: {password}")
            print("\n如需修改连接参数，请编辑脚本中的相应变量")
            return
    
    # 检查是否跳过确认
    confirm = '--confirm' in sys.argv
    
    # 创建清理器实例
    cleaner = DatabaseCleaner(uri=uri, username=username, password=password)
    
    try:
        # 连接数据库
        if not cleaner.connect():
            return
        
        # 执行清理
        success = cleaner.delete_empty_type_entities(confirm=confirm)
        
        if success:
            # 验证结果
            cleaner.verify_cleanup()
        
    except KeyboardInterrupt:
        print("\n❌ 操作被用户中断")
    except Exception as e:
        print(f"❌ 程序执行失败: {e}")
    finally:
        # 关闭连接
        cleaner.close()

if __name__ == "__main__":
    main()