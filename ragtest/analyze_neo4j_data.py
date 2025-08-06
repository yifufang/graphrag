#!/usr/bin/env python3
"""
分析Neo4j数据库中的数据分布
"""

from neo4j import GraphDatabase
import pandas as pd
from collections import defaultdict

class Neo4jAnalyzer:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="password"):
        """初始化Neo4j连接"""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        """关闭连接"""
        self.driver.close()
        
    def run_query(self, query, parameters=None):
        """执行Cypher查询"""
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return list(result)
    
    def get_database_info(self):
        """获取数据库基本信息"""
        print("🔍 数据库基本信息")
        print("=" * 50)
        
        # 获取节点总数
        result = self.run_query("MATCH (n) RETURN count(n) as total_nodes")
        total_nodes = result[0]['total_nodes']
        print(f"总节点数: {total_nodes:,}")
        
        # 获取关系总数
        result = self.run_query("MATCH ()-[r]->() RETURN count(r) as total_relationships")
        total_relationships = result[0]['total_relationships']
        print(f"总关系数: {total_relationships:,}")
        
        # 获取标签数量
        result = self.run_query("CALL db.labels() YIELD label RETURN count(label) as total_labels")
        total_labels = result[0]['total_labels']
        print(f"标签种类数: {total_labels}")
        
        # 获取关系类型数量
        result = self.run_query("CALL db.relationshipTypes() YIELD relationshipType RETURN count(relationshipType) as total_types")
        total_types = result[0]['total_types']
        print(f"关系类型数: {total_types}")
        
        return {
            'total_nodes': total_nodes,
            'total_relationships': total_relationships,
            'total_labels': total_labels,
            'total_types': total_types
        }
    
    def analyze_node_labels(self):
        """分析节点标签分布"""
        print("\n🏷️ 节点标签分布")
        print("=" * 50)
        
        # 获取所有标签及其节点数量
        query = """
        CALL db.labels() YIELD label
        MATCH (n:`${label}`)
        RETURN label, count(n) as count
        ORDER BY count DESC
        """
        
        # 由于上面的查询有语法问题，我们分步执行
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
        
        for item in label_counts:
            percentage = item['count'] / total_nodes * 100
            print(f"{item['label']:<20}: {item['count']:>8,} ({percentage:>5.1f}%)")
        
        return label_counts
    
    def analyze_relationship_types(self):
        """分析关系类型分布"""
        print("\n🔗 关系类型分布")
        print("=" * 50)
        
        # 获取所有关系类型及其数量
        rel_types = self.run_query("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
        
        rel_counts = []
        for record in rel_types:
            rel_type = record['relationshipType']
            result = self.run_query(f"MATCH ()-[r:`{rel_type}`]->() RETURN count(r) as count")
            count = result[0]['count']
            rel_counts.append({'type': rel_type, 'count': count})
        
        # 按数量排序
        rel_counts.sort(key=lambda x: x['count'], reverse=True)
        
        total_rels = sum(item['count'] for item in rel_counts)
        
        for item in rel_counts:
            percentage = item['count'] / total_rels * 100
            print(f"{item['type']:<20}: {item['count']:>8,} ({percentage:>5.1f}%)")
        
        return rel_counts
    
    def analyze_node_properties(self):
        """分析节点属性结构"""
        print("\n📊 节点属性结构分析")
        print("=" * 50)
        
        # 获取所有标签
        labels = self.run_query("CALL db.labels() YIELD label RETURN label")
        
        for record in labels:
            label = record['label']
            print(f"\n🏷️ 标签: {label}")
            print("-" * 30)
            
            # 获取该标签下节点的属性
            query = f"""
            MATCH (n:`{label}`)
            RETURN keys(n) as properties
            LIMIT 100
            """
            
            results = self.run_query(query)
            
            if results:
                # 统计属性出现频率
                prop_counts = defaultdict(int)
                for result in results:
                    props = result['properties']
                    for prop in props:
                        prop_counts[prop] += 1
                
                # 显示属性统计
                for prop, count in sorted(prop_counts.items()):
                    percentage = count / len(results) * 100
                    print(f"  {prop:<20}: {count:>3} ({percentage:>5.1f}%)")
                
                # 移除示例数据打印
    
    def analyze_relationship_properties(self):
        """分析关系属性结构"""
        print("\n🔗 关系属性结构分析")
        print("=" * 50)
        
        # 获取所有关系类型
        rel_types = self.run_query("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
        
        for record in rel_types:
            rel_type = record['relationshipType']
            print(f"\n🔗 关系类型: {rel_type}")
            print("-" * 30)
            
            # 获取该关系类型的属性
            query = f"""
            MATCH ()-[r:`{rel_type}`]->()
            RETURN keys(r) as properties
            LIMIT 100
            """
            
            results = self.run_query(query)
            
            if results:
                # 统计属性出现频率
                prop_counts = defaultdict(int)
                for result in results:
                    props = result['properties']
                    for prop in props:
                        prop_counts[prop] += 1
                
                # 显示属性统计
                for prop, count in sorted(prop_counts.items()):
                    percentage = count / len(results) * 100
                    print(f"  {prop:<20}: {count:>3} ({percentage:>5.1f}%)")
                
                # 移除示例数据打印
    
    def analyze_connectivity(self):
        """分析图的连通性"""
        print("\n🌐 图连通性分析")
        print("=" * 50)
        
        # 计算孤立节点
        isolated_query = """
        MATCH (n)
        WHERE NOT (n)--()
        RETURN count(n) as isolated_count
        """
        isolated_result = self.run_query(isolated_query)
        isolated_count = isolated_result[0]['isolated_count']
        print(f"孤立节点数: {isolated_count:,}")
        
        # 计算连通分量
        components_query = """
        CALL gds.alpha.scc.stream('*')
        YIELD componentId, nodeId
        RETURN componentId, count(nodeId) as component_size
        ORDER BY component_size DESC
        LIMIT 10
        """
        
        try:
            components = self.run_query(components_query)
            print(f"\n最大连通分量:")
            for i, comp in enumerate(components[:5], 1):
                print(f"  分量{i}: {comp['component_size']:,} 个节点")
        except Exception as e:
            print(f"  无法计算连通分量: {e}")
    
    def get_sample_relationships(self):
        """获取关系示例"""
        print("\n🔗 关系示例")
        print("=" * 50)
        
        # 获取每种关系类型的示例
        rel_types = self.run_query("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
        
        for record in rel_types:
            rel_type = record['relationshipType']
            print(f"\n🔗 {rel_type} 关系示例:")
            print("-" * 30)
            
            query = f"""
            MATCH (a)-[r:`{rel_type}`]->(b)
            RETURN a.name as source, b.name as target, r
            LIMIT 3
            """
            
            results = self.run_query(query)
            
            for i, result in enumerate(results, 1):
                source = result.get('source', 'N/A')
                target = result.get('target', 'N/A')
                rel_props = dict(result['r'])
                print(f"  示例{i}: {source} --[{rel_type}]--> {target}")
                # 移除属性打印
                # if rel_props:
                #     print(f"      属性: {rel_props}")
    
    def run_full_analysis(self):
        """运行完整分析"""
        print("🔍 Neo4j数据库分析报告")
        print("=" * 60)
        
        try:
            # 基本信息
            self.get_database_info()
            
            # 节点标签分析
            self.analyze_node_labels()
            
            # 关系类型分析
            self.analyze_relationship_types()
            
            # 节点属性分析
            self.analyze_node_properties()
            
            # 关系属性分析
            self.analyze_relationship_properties()
            
            # 连通性分析
            self.analyze_connectivity()
            
            # 关系示例
            self.get_sample_relationships()
            
        except Exception as e:
            print(f"❌ 分析过程中出现错误: {e}")
        finally:
            self.close()

def main():
    """主函数"""
    # 默认连接参数，可以根据实际情况修改
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "password"
    
    print("🔍 开始分析Neo4j数据库...")
    print("请确保Neo4j服务正在运行")
    print("=" * 60)
    
    try:
        analyzer = Neo4jAnalyzer(uri, user, password)
        analyzer.run_full_analysis()
        print("\n✅ 分析完成!")
        
    except Exception as e:
        print(f"❌ 连接失败: {e}")
        print("请检查:")
        print("1. Neo4j服务是否正在运行")
        print("2. 连接参数是否正确")
        print("3. 防火墙设置")

if __name__ == "__main__":
    main() 