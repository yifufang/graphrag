#!/usr/bin/env python3
"""
直接修改Neo4j数据库中的关系类型
从description中提取括号内的内容作为新的关系类型
"""

from neo4j import GraphDatabase
import re

class Neo4jRelationshipUpdater:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="password", database="neo4j"):
        """初始化Neo4j连接"""
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self.driver = None
        
    def connect(self) -> bool:
        """连接到Neo4j数据库"""
        try:
            print(f"🔌 连接到Neo4j数据库: {self.database}...")
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            
            # 测试连接
            with self.driver.session(database=self.database) as session:
                result = session.run("RETURN 'Hello Neo4j' as message")
                record = result.single()
                if record:
                    message = record["message"]
                    print(f"✅ 连接成功: {message}")
                else:
                    print("✅ 连接成功")
            return True
            
        except Exception as e:
            print(f"❌ 连接失败: {e}")
            return False
        
    def close(self):
        """关闭连接"""
        if self.driver:
            self.driver.close()
            print("🔌 数据库连接已关闭")
        
    def list_databases(self):
        """列出可用的数据库"""
        try:
            print("📋 可用数据库列表:")
            # 连接到默认数据库来获取数据库列表
            temp_driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            with temp_driver.session() as session:
                result = session.run("SHOW DATABASES")
                for record in result:
                    db_name = record["name"]
                    db_role = record.get("role", "unknown")
                    db_address = record.get("address", "unknown")
                    print(f"   - {db_name} (role: {db_role}, address: {db_address})")
            temp_driver.close()
        except Exception as e:
            print(f"❌ 获取数据库列表失败: {e}")
            print("   请确保Neo4j服务正在运行")
        
    def run_query(self, query, parameters=None):
        """执行Cypher查询"""
        if self.driver is None:
            raise RuntimeError("Database driver not initialized. Call connect() first.")
        with self.driver.session(database=self.database) as session:
            result = session.run(query, parameters or {})
            return list(result)
    
    def get_all_relationships(self):
        """获取所有关系及其描述"""
        print("🔍 获取所有关系...")
        
        query = """
        MATCH ()-[r]->()
        RETURN id(r) as rel_id, type(r) as rel_type, r.description as description
        """
        
        results = self.run_query(query)
        print(f"✅ 找到 {len(results):,} 条关系")
        
        return results
    
    def extract_relationship_type_from_description(self, description):
        """从描述中提取关系类型"""
        if not description:
            return None
            
        # 查找方括号内的内容
        bracket_pattern = r'\[([^\]]+)\]'
        matches = re.findall(bracket_pattern, description)
        
        if matches:
            rel_type = matches[0].strip()
            # 移除白名单限制，允许抽取所有关系类型
            # allowed = {"表现为", "可见于", "导致", "治疗", "来源于", "相关", "适用于"}
            # return rel_type if rel_type in allowed else None
            return rel_type if rel_type else None  # 只要不为空就返回
        return None
    
    def update_relationship_type(self, rel_id, new_type):
        """更新关系类型"""
        # 先删除旧关系，再创建新关系
        # 注意：Neo4j不允许参数化关系类型，需要使用字符串拼接
        query = f"""
        MATCH (a)-[r]->(b)
        WHERE id(r) = {rel_id}
        WITH a, b, r, properties(r) as props
        DELETE r
        CREATE (a)-[r2:`{new_type}`]->(b)
        SET r2 = props
        RETURN r2
        """
        
        try:
            result = self.run_query(query)
            return True
        except Exception as e:
            print(f"❌ 更新关系 {rel_id} 失败: {e}")
            return False
    
    def process_relationships(self):
        """处理所有关系"""
        print("🔄 开始处理关系类型...")
        print("=" * 50)
        
        # 获取所有关系
        relationships = self.get_all_relationships()
        
        updated_count = 0
        skipped_count = 0
        error_count = 0
        
        for rel in relationships:
            rel_id = rel['rel_id']
            current_type = rel['rel_type']
            description = rel.get('description', '')
            
            # 提取新的关系类型
            new_type = self.extract_relationship_type_from_description(description)
            
            if new_type:
                print(f"🔄 更新: {current_type} -> {new_type}")
                print(f"   描述: {description[:50]}...")
                
                if self.update_relationship_type(rel_id, new_type):
                    updated_count += 1
                    print(f"   ✅ 成功更新")
                else:
                    error_count += 1
                    print(f"   ❌ 更新失败")
            else:
                skipped_count += 1
                print(f"⏭️  跳过: {current_type} (无括号内容)")
        
        print(f"\n📊 处理结果:")
        print("=" * 50)
        print(f"✅ 成功更新: {updated_count:,} 条关系")
        print(f"⏭️  跳过处理: {skipped_count:,} 条关系")
        print(f"❌ 更新失败: {error_count:,} 条关系")
        
        return updated_count, skipped_count, error_count
    
    def verify_updates(self):
        """验证更新结果"""
        print("\n🔍 验证更新结果...")
        print("=" * 50)
        
        # 获取更新后的关系类型分布
        query = """
        MATCH ()-[r]->()
        RETURN type(r) as rel_type, count(r) as count
        ORDER BY count DESC
        """
        
        results = self.run_query(query)
        
        print("关系类型分布:")
        for result in results:
            rel_type = result['rel_type']
            count = result['count']
            print(f"  {rel_type:<20}: {count:>6,}")
        
        # 检查是否还有RELATED_TO类型
        related_to_query = """
        MATCH ()-[r:RELATED_TO]->()
        RETURN count(r) as count
        """
        
        related_to_result = self.run_query(related_to_query)
        related_to_count = related_to_result[0]['count']
        
        if related_to_count > 0:
            print(f"\n⚠️  仍有 {related_to_count:,} 条RELATED_TO关系未更新")
        else:
            print(f"\n✅ 所有RELATED_TO关系已更新完成")
    
    def run_full_update(self):
        """运行完整更新流程"""
        print("🔄 Neo4j关系类型更新工具")
        print("=" * 60)
        
        try:
            # 先连接数据库
            if not self.connect():
                print("❌ 无法连接到数据库，退出")
                return
            
            # 处理关系
            updated, skipped, errors = self.process_relationships()
            
            # 验证结果
            self.verify_updates()
            
            print(f"\n✅ 更新完成!")
            print(f"📝 总结:")
            print(f"   - 更新了 {updated:,} 条关系")
            print(f"   - 跳过了 {skipped:,} 条关系")
            print(f"   - 失败了 {errors:,} 条关系")
            
        except Exception as e:
            print(f"❌ 更新过程中出现错误: {e}")
        finally:
            self.close()

def main():
    """主函数"""
    print("🔄 开始更新Neo4j关系类型...")
    print("规则:")
    print("1. 从description中提取[括号]内的内容")
    print("2. 将提取的内容作为新的关系类型")
    print("3. 如果没有括号内容则跳过")
    print("=" * 60)
    
    try:
        updater = Neo4jRelationshipUpdater()
        updater.run_full_update()
        
    except Exception as e:
        print(f"❌ 连接失败: {e}")
        print("请检查Neo4j服务是否运行")

if __name__ == "__main__":
    main() 