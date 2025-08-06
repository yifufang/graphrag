#!/usr/bin/env python3
"""
清洗Neo4j数据库中的非预定义实体节点
"""

from neo4j import GraphDatabase

class Neo4jCleaner:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="password"):
        """初始化Neo4j连接"""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
        # 预定义的中医实体类型
        self.predefined_types = [
            '药材', '方剂', '疾病', '症状', '医家', '功效', 
            '病因', '脉象', '诊断方法', '经络', '穴位', '脏腑'
        ]
        
    def close(self):
        """关闭连接"""
        self.driver.close()
        
    def run_query(self, query, parameters=None):
        """执行Cypher查询"""
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return list(result)
    
    def get_entity_statistics(self):
        """获取实体统计信息"""
        print("📊 清洗前实体统计")
        print("=" * 50)
        
        # 获取所有标签及其节点数量
        labels = self.run_query("CALL db.labels() YIELD label RETURN label")
        
        label_counts = []
        for record in labels:
            label = record['label']
            result = self.run_query(f"MATCH (n:`{label}`) RETURN count(n) as count")
            count = result[0]['count']
            label_counts.append({'label': label, 'count': count})
        
        # 按数量排序
        label_counts.sort(key=lambda x: x['count'], reverse=True)
        
        total_nodes = sum(item['count'] for item in label_counts)
        
        print(f"总节点数: {total_nodes:,}")
        print("\n节点标签分布:")
        for item in label_counts:
            percentage = item['count'] / total_nodes * 100
            print(f"  {item['label']:<20}: {item['count']:>8,} ({percentage:>5.1f}%)")
        
        return label_counts
    
    def analyze_entity_types(self):
        """分析实体类型分布"""
        print("\n🔍 实体类型分析")
        print("=" * 50)
        
        # 获取所有标签
        labels = self.run_query("CALL db.labels() YIELD label RETURN label")
        label_list = [record['label'] for record in labels]
        
        predefined_count = 0
        unknown_count = 0
        other_count = 0
        
        for label in label_list:
            result = self.run_query(f"MATCH (n:`{label}`) RETURN count(n) as count")
            count = result[0]['count']
            
            if label in self.predefined_types:
                predefined_count += count
                print(f"✅ 预定义类型: {label:<15} - {count:>6,} 个节点")
            elif label.lower() == 'unknown':
                unknown_count += count
                print(f"⏭️  Unknown类型: {label:<15} - {count:>6,} 个节点 (将跳过)")
            else:
                other_count += count
                print(f"❓ 其他类型: {label:<15} - {count:>6,} 个节点 (需要检查)")
        
        print(f"\n📈 分类统计:")
        print(f"  预定义类型: {predefined_count:,} 个节点")
        print(f"  Unknown类型: {unknown_count:,} 个节点")
        print(f"  其他类型: {other_count:,} 个节点")
        
        return {
            'predefined': predefined_count,
            'unknown': unknown_count,
            'other': other_count
        }
    
    def check_entity_connections(self):
        """检查非预定义实体的连接情况"""
        print("\n🔗 检查非预定义实体连接情况")
        print("=" * 50)
        
        # 获取所有标签
        labels = self.run_query("CALL db.labels() YIELD label RETURN label")
        label_list = [record['label'] for record in labels]
        
        connected_entities = []
        isolated_entities = []
        
        for label in label_list:
            if label in self.predefined_types or label.lower() == 'unknown':
                continue
                
            # 检查该标签下的节点是否有关系连接
            query = f"""
            MATCH (n:`{label}`)
            WHERE (n)--()
            RETURN count(n) as connected_count
            """
            connected_result = self.run_query(query)
            connected_count = connected_result[0]['connected_count']
            
            # 检查该标签下的总节点数
            total_query = f"MATCH (n:`{label}`) RETURN count(n) as total_count"
            total_result = self.run_query(total_query)
            total_count = total_result[0]['total_count']
            
            isolated_count = total_count - connected_count
            
            if connected_count > 0:
                connected_entities.append({
                    'label': label,
                    'connected': connected_count,
                    'isolated': isolated_count,
                    'total': total_count
                })
                print(f"✅ {label:<15}: {connected_count:>6,} 个有连接, {isolated_count:>6,} 个孤立")
            else:
                isolated_entities.append({
                    'label': label,
                    'connected': 0,
                    'isolated': total_count,
                    'total': total_count
                })
                print(f"❌ {label:<15}: {total_count:>6,} 个全部孤立")
        
        return connected_entities, isolated_entities
    
    def clean_entities(self):
        """清洗实体节点"""
        print("\n🧹 开始清洗实体节点")
        print("=" * 50)
        
        # 获取孤立的其他类型实体
        labels = self.run_query("CALL db.labels() YIELD label RETURN label")
        label_list = [record['label'] for record in labels]
        
        nodes_to_delete = 0
        labels_to_process = []
        
        for label in label_list:
            if label in self.predefined_types or label.lower() == 'unknown':
                continue
                
            # 检查孤立节点
            isolated_query = f"""
            MATCH (n:`{label}`)
            WHERE NOT (n)--()
            RETURN count(n) as isolated_count
            """
            isolated_result = self.run_query(isolated_query)
            isolated_count = isolated_result[0]['isolated_count']
            
            if isolated_count > 0:
                labels_to_process.append({
                    'label': label,
                    'isolated_count': isolated_count
                })
                nodes_to_delete += isolated_count
                print(f"🗑️  将删除 {label} 标签下的 {isolated_count:,} 个孤立节点")
        
        if not labels_to_process:
            print("✅ 没有需要删除的孤立节点")
            return
        
        print(f"\n⚠️  确认删除 {nodes_to_delete:,} 个孤立节点? (y/N): ", end="")
        confirm = input().strip().lower()
        
        if confirm != 'y':
            print("❌ 操作已取消")
            return
        
        # 执行删除操作
        deleted_count = 0
        for item in labels_to_process:
            label = item['label']
            isolated_count = item['isolated_count']
            
            delete_query = f"""
            MATCH (n:`{label}`)
            WHERE NOT (n)--()
            DELETE n
            RETURN count(n) as deleted_count
            """
            
            try:
                result = self.run_query(delete_query)
                deleted_count += isolated_count
                print(f"✅ 已删除 {label} 标签下的 {isolated_count:,} 个孤立节点")
            except Exception as e:
                print(f"❌ 删除 {label} 标签节点时出错: {e}")
        
        print(f"\n✅ 清洗完成! 共删除 {deleted_count:,} 个孤立节点")
    
    def verify_cleaning_results(self):
        """验证清洗结果"""
        print("\n🔍 验证清洗结果")
        print("=" * 50)
        
        # 重新统计节点
        labels = self.run_query("CALL db.labels() YIELD label RETURN label")
        
        total_nodes = 0
        remaining_other_types = []
        
        for record in labels:
            label = record['label']
            result = self.run_query(f"MATCH (n:`{label}`) RETURN count(n) as count")
            count = result[0]['count']
            total_nodes += count
            
            if label not in self.predefined_types and label.lower() != 'unknown':
                remaining_other_types.append({
                    'label': label,
                    'count': count
                })
        
        print(f"清洗后总节点数: {total_nodes:,}")
        
        if remaining_other_types:
            print("\n剩余的其他类型实体:")
            for item in remaining_other_types:
                print(f"  {item['label']:<15}: {item['count']:>6,} 个节点")
        else:
            print("\n✅ 所有非预定义实体已清理完成")
    
    def run_full_cleaning(self):
        """运行完整清洗流程"""
        print("🧹 Neo4j实体清洗工具")
        print("=" * 60)
        
        try:
            # 1. 获取清洗前统计
            self.get_entity_statistics()
            
            # 2. 分析实体类型
            self.analyze_entity_types()
            
            # 3. 检查连接情况
            connected, isolated = self.check_entity_connections()
            
            # 4. 执行清洗
            self.clean_entities()
            
            # 5. 验证结果
            self.verify_cleaning_results()
            
        except Exception as e:
            print(f"❌ 清洗过程中出现错误: {e}")
        finally:
            self.close()

def main():
    """主函数"""
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "password"
    
    print("🧹 开始清洗Neo4j数据库...")
    print("清洗规则:")
    print("1. Unknown类型的实体将被跳过")
    print("2. 预定义类型的实体将被保留")
    print("3. 其他类型的孤立实体将被删除")
    print("4. 其他类型的有连接实体将被保留")
    print("=" * 60)
    
    try:
        cleaner = Neo4jCleaner(uri, user, password)
        cleaner.run_full_cleaning()
        print("\n✅ 清洗完成!")
        
    except Exception as e:
        print(f"❌ 清洗失败: {e}")

if __name__ == "__main__":
    main() 