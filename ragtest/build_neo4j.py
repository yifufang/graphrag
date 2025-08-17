#!/usr/bin/env python3
"""
构建中医知识图谱Neo4j数据库
从GraphRAG输出的entities.parquet和relationships.parquet文件构建Neo4j图数据库
支持不同实体类型的颜色显示和可视化
"""

import pandas as pd
import numpy as np
from neo4j import GraphDatabase
import json
import sys
import time
from typing import Dict, List, Any, Optional, Tuple
import re
import warnings
warnings.filterwarnings('ignore')

class TCMNeo4jBuilder:
    """中医知识图谱Neo4j构建器"""
    
    def __init__(self, uri: str = "neo4j://localhost", username: str = "neo4j", password: str = "password", database: str = "neo4j"):
        """初始化Neo4j连接"""
        self.uri = uri
        self.username = username
        self.password = password
        self.database = database
        self.driver = None
        
        # 动态类型映射将在load_entities时生成
        self.type_to_label = {}
        self.actual_types = set()

    # ======== 辅助解析函数 ========
    @staticmethod
    def _extract_source_snippet(text: str) -> Tuple[str, str]:
        """从描述中抽取来源原文片段，返回(清理后描述, 原文片段)"""
        if not text:
            return "", ""
        snippet = ""
        # 匹配：来源原文片段：<不超过若干字符>
        m = re.search(r"来源原文片段[:：]\s*([^\n]{0,120})", text)
        if m:
            snippet = m.group(1).strip()
            # 去掉整段“来源原文片段：...”
            text = re.sub(r"来源原文片段[:：]\s*[^\n]{0,120}", "", text).strip()
        return text.strip(), snippet

    @staticmethod
    def _extract_kv_properties(text: str) -> Dict[str, Any]:
        """从描述中解析 键=值/键:值/键：值，分隔符为；; 换行 等，返回字典"""
        if not text:
            return {}
        props: Dict[str, Any] = {}
        # 提前替换中文分号为统一分隔
        normalized = text.replace("；", ";")
        # 提取可能的键值对（中文/英文键名），避免吞噬整句，只匹配到分号或行尾
        for part in normalized.split(";"):
            part = part.strip()
            if not part:
                continue
            m = re.match(r"([A-Za-z_\u4e00-\u9fa5]+)\s*[:：=]\s*(.+)$", part)
            if m:
                key = m.group(1).strip()
                val = m.group(2).strip()
                # 过长值截断，避免无界扩张
                if len(val) > 300:
                    val = val[:300]
                # 避免覆盖保留字段
                if key not in {"id", "name", "type", "description", "human_readable_id", "degree", "source_snippet", "raw_description"}:
                    props[key] = val
        return props

    @staticmethod
    def _extract_rel_fields(description: str) -> Tuple[str, str, Dict[str, Any]]:
        """从关系描述里抽取 [类型]、理由 与 KV属性"""
        if not description:
            return "", "", {}
        desc = description.strip()
        kind = ""
        reason = desc
        m = re.match(r"\s*\[([^\]]+)\]\s*(.*)$", desc)
        if m:
            kind = m.group(1).strip()
            reason = m.group(2).strip()
        # 从理由中解析键值对
        kv_props = TCMNeo4jBuilder._extract_kv_properties(reason)
        return kind, reason, kv_props
    
    def connect(self) -> bool:
        """连接到Neo4j数据库"""
        try:
            print(f"🔌 连接到Neo4j数据库: {self.database}...")
            self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
            
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
        """关闭数据库连接"""
        if self.driver:
            self.driver.close()
            print("🔌 数据库连接已关闭")
    
    def list_databases(self):
        """列出可用的数据库"""
        try:
            print("📋 可用数据库列表:")
            # 连接到默认数据库来获取数据库列表
            temp_driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
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
    
    def clear_database(self) -> bool:
        """清空数据库"""
        try:
            print("🗑️  清空数据库...")
            if not self.driver:
                print("❌ 数据库连接未建立")
                return False
            with self.driver.session(database=self.database) as session:
                # 删除所有数据
                session.run("MATCH (n) DETACH DELETE n")
                print("✅ 数据库已清空")
            return True
        except Exception as e:
            print(f"❌ 清空数据库失败: {e}")
            return False
    
    def create_constraints_and_indexes(self):
        """创建约束和索引"""
        print("📊 创建约束和索引...")
        
        if not self.driver:
            print("❌ 数据库连接未建立")
            return
        
        # 为所有可能的实体类型创建约束和索引
        entity_labels = set(self.type_to_label.values())
        entity_labels.add("Entity")  # 添加默认标签
        
        constraints_and_indexes = []
        
        # 为每个实体类型创建约束和索引
        for label in entity_labels:
            constraints_and_indexes.extend([
                f"CREATE CONSTRAINT {label.lower()}_id_unique IF NOT EXISTS FOR (e:{label}) REQUIRE e.id IS UNIQUE",
                f"CREATE INDEX {label.lower()}_name_index IF NOT EXISTS FOR (e:{label}) ON (e.name)",
                f"CREATE INDEX {label.lower()}_type_index IF NOT EXISTS FOR (e:{label}) ON (e.type)",
            ])
        
        # 关系索引
        constraints_and_indexes.extend([
            "CREATE INDEX relationship_weight_index IF NOT EXISTS FOR ()-[r:RELATED_TO]-() ON (r.weight)",
            "CREATE INDEX relationship_rank_index IF NOT EXISTS FOR ()-[r:RELATED_TO]-() ON (r.rank)",
            "CREATE INDEX relationship_kind_index IF NOT EXISTS FOR ()-[r:RELATED_TO]-() ON (r.kind)",
            "CREATE INDEX relationship_reason_index IF NOT EXISTS FOR ()-[r:RELATED_TO]-() ON (r.reason)"
        ])
        
        with self.driver.session(database=self.database) as session:
            for cmd in constraints_and_indexes:
                try:
                    session.run(cmd)
                    print(f"   ✅ {cmd.split()[1]} 创建成功")
                except Exception as e:
                    print(f"   ⚠️  {cmd}: {e}")
    
    def load_entities(self, entities_file: str = "./output2/entities.parquet") -> pd.DataFrame:
        """加载实体数据"""
        print(f"📚 加载实体数据: {entities_file}")
        try:
            # 读取实体数据
            entities_df = pd.read_parquet(entities_file)
            print(f"✅ 加载了 {len(entities_df)} 个实体")
            
            # 显示实体类型统计
            if 'type' in entities_df.columns:
                type_counts = entities_df['type'].value_counts()
                print("📊 实体类型分布:")
                for entity_type, count in type_counts.head(10).items():
                    print(f"   {entity_type}: {count}")
                
                # 动态生成类型映射
                unique_types = entities_df['type'].unique()
                self.generate_type_mappings(unique_types)
            
            return entities_df
            
        except Exception as e:
            print(f"❌ 加载实体数据失败: {e}")
            return pd.DataFrame()
    
    def load_relationships(self, relationships_file: str = "./output2/relationships.parquet") -> pd.DataFrame:
        """加载关系数据"""
        print(f"🔗 加载关系数据: {relationships_file}")
        try:
            # 读取关系数据
            relationships_df = pd.read_parquet(relationships_file)
            print(f"✅ 加载了 {len(relationships_df)} 个关系")
            
            # 显示关系权重分布
            if 'weight' in relationships_df.columns:
                print(f"📊 关系权重统计:")
                print(f"   平均权重: {relationships_df['weight'].mean():.3f}")
                print(f"   最大权重: {relationships_df['weight'].max():.3f}")
                print(f"   最小权重: {relationships_df['weight'].min():.3f}")
            
            return relationships_df
            
        except Exception as e:
            print(f"❌ 加载关系数据失败: {e}")
            return pd.DataFrame()
    
    def generate_type_mappings(self, entity_types):
        """根据实际的entity types生成Neo4j标签映射"""
        print("📋 动态生成实体类型映射...")
        
        # 清空现有映射
        self.type_to_label.clear()
        
        type_count = 0
        for entity_type in entity_types:
            if pd.isna(entity_type) or str(entity_type).strip() == '':
                self.type_to_label[''] = 'Unknown'
                continue
                
            clean_type = str(entity_type).strip().strip('"')
            if clean_type:
                # 生成合适的Neo4j标签
                label = self._generate_neo4j_label(clean_type)
                self.type_to_label[clean_type] = label
                type_count += 1
                
        print(f"   生成了 {type_count} 个类型映射")
        # 显示前10个映射
        for i, (orig_type, label) in enumerate(list(self.type_to_label.items())[:10]):
            if orig_type:  # 不显示空值映射
                print(f"   {orig_type} → {label}")
        if len(self.type_to_label) > 10:
            print(f"   ... 还有 {len(self.type_to_label) - 10} 个映射")
    
    def _generate_neo4j_label(self, chinese_type: str) -> str:
        """为中文类型生成合适的Neo4j标签"""
        import re
        
        # Neo4j支持中文标签，只需要简单清理即可
        # 移除特殊字符，保留中文、英文、数字
        clean_name = re.sub(r'[（）()\[\]{}，。、/\\<>|*?:"\'`~!@#$%^&+=\s]', '', chinese_type)
        
        # 如果清理后不为空，直接使用
        if clean_name:
            return clean_name
        else:
            # 空值或无效名称
            return "Unknown"
    
    def get_entity_label(self, entity_type: str) -> str:
        """根据实体类型获取Neo4j标签"""
        if not entity_type or pd.isna(entity_type) or str(entity_type).strip() == '':
            return "Unknown"  # 空值或无效类型
        
        # 清理类型字符串
        clean_type = str(entity_type).strip().strip('"')
        
        # 查找映射，如果没有就使用默认
        return self.type_to_label.get(clean_type, "Entity")
    
    def create_entities(self, entities_df: pd.DataFrame, batch_size: int = 1000):
        """批量创建实体节点"""
        print(f"🏗️  创建实体节点 (批次大小: {batch_size})...")
        
        if not self.driver:
            print("❌ 数据库连接未建立")
            return
        
        total_entities = len(entities_df)
        created_count = 0
        
        with self.driver.session(database=self.database) as session:
            for i in range(0, total_entities, batch_size):
                batch = entities_df.iloc[i:i+batch_size]
                
                # 按标签分组实体数据
                entities_by_label = {}
                for _, row in batch.iterrows():
                    entity_type = str(row.get('type', '')).strip().strip('"') if pd.notna(row.get('type')) else ''
                    label = self.get_entity_label(entity_type)
                    
                    original_desc = str(row.get('description', '')) if pd.notna(row.get('description')) else ''
                    # 提取来源原文片段并清理描述
                    cleaned_desc, source_snippet = self._extract_source_snippet(original_desc[:1200])
                    # 提取潜在KV属性
                    kv_props = self._extract_kv_properties(cleaned_desc)

                    entity_data = {
                        'id': str(row.get('id', '')),
                        'name': str(row.get('title', row.get('name', ''))).strip().strip('"'),
                        'type': entity_type,
                        'description': cleaned_desc[:1000],
                        'raw_description': original_desc[:1200],
                        'source_snippet': source_snippet,
                        'human_readable_id': int(row.get('human_readable_id', 0)) if pd.notna(row.get('human_readable_id')) else 0,
                        'degree': int(row.get('degree', 0)) if pd.notna(row.get('degree')) else 0,
                        'extra_props': kv_props
                    }
                    
                    if label not in entities_by_label:
                        entities_by_label[label] = []
                    entities_by_label[label].append(entity_data)
                
                # 按标签批量插入
                batch_created = 0
                try:
                    for label, label_entities in entities_by_label.items():
                        cypher_query = f"""
                            UNWIND $entities as entity
                            MERGE (e:{label} {{id: entity.id}})
                            SET e.name = entity.name,
                                e.name = entity.name,
                                e.type = entity.type,
                                e.description = entity.description,
                                e.raw_description = entity.raw_description,
                                e.source_snippet = entity.source_snippet,
                                e.human_readable_id = entity.human_readable_id,
                                e.degree = entity.degree
                            SET e += entity.extra_props
                        """
                        session.run(cypher_query, entities=label_entities)
                        batch_created += len(label_entities)
                        print(f"   📋 创建了 {len(label_entities)} 个 {label} 实体")
                    
                    created_count += batch_created
                    print(f"   ✅ 已创建 {created_count}/{total_entities} 个实体 ({created_count/total_entities*100:.1f}%)")
                    
                except Exception as e:
                    print(f"   ❌ 批次 {i//batch_size + 1} 创建失败: {e}")
        
        print(f"🎉 实体创建完成! 总计: {created_count}")
        
        # 显示实体类型统计
        self.show_entity_statistics()
    
    def show_entity_statistics(self):
        """显示数据库中各类型实体的统计信息"""
        print("📊 实体类型统计:")
        
        try:
            with self.driver.session(database=self.database) as session:
                # 获取所有标签及其实体数量
                result = session.run("CALL db.labels() YIELD label RETURN label")
                labels = [record["label"] for record in result]
                
                total_entities = 0
                for label in sorted(labels):
                    count_result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                    count = count_result.single()["count"]
                    if count > 0:
                        # 查找对应的中文类型
                        chinese_type = ""
                        for zh_type, en_label in self.type_to_label.items():
                            if en_label == label:
                                chinese_type = f" ({zh_type})"
                                break
                        print(f"   {label}{chinese_type}: {count}")
                        total_entities += count
                
                print(f"   总计: {total_entities} 个实体")
                
        except Exception as e:
            print(f"   ❌ 获取统计信息失败: {e}")
    
    def create_relationships(self, relationships_df: pd.DataFrame, batch_size: int = 1000):
        """批量创建关系"""
        print(f"🔗 创建关系 (批次大小: {batch_size})...")
        
        total_relationships = len(relationships_df)
        created_count = 0
        
        with self.driver.session(database=self.database) as session:
            for i in range(0, total_relationships, batch_size):
                batch = relationships_df.iloc[i:i+batch_size]
                
                # 准备批次数据
                relationships_data = []
                for _, row in batch.iterrows():
                    relationship_data = {
                        'source_name': str(row.get('source', '')).strip().strip('"'),
                        'target_name': str(row.get('target', '')).strip().strip('"'),
                        'id': str(row.get('id', '')),
                        'description': str(row.get('description', ''))[:800] if pd.notna(row.get('description')) else '',
                        'weight': float(row.get('weight', 1.0)) if pd.notna(row.get('weight')) else 1.0,
                        'rank': int(row.get('rank', 0)) if pd.notna(row.get('rank')) else 0
                    }
                    # 解析关系类型、理由与KV属性
                    kind, reason, rel_kv = self._extract_rel_fields(relationship_data['description'])
                    relationship_data['kind'] = kind
                    relationship_data['reason'] = reason
                    relationship_data['extra_props'] = rel_kv
                    relationships_data.append(relationship_data)
                
                # 批量插入关系 - 使用通用标签匹配
                try:
                    # 从描述中解析关系类型与理由（已在Python层完成），并写入属性
                    session.run("""
                        UNWIND $relationships as rel
                        MATCH (source {name: rel.source_name})
                        MATCH (target {name: rel.target_name})
                        CREATE (source)-[r:RELATED_TO]->(target)
                        SET r.id = rel.id,
                            r.description = rel.description,
                            r.kind = rel.kind,
                            r.reason = rel.reason,
                            r.weight = rel.weight,
                            r.rank = rel.rank
                        SET r += rel.extra_props
                    """, relationships=relationships_data)
                    
                    created_count += len(batch)
                    print(f"   ✅ 已创建 {created_count}/{total_relationships} 个关系 ({created_count/total_relationships*100:.1f}%)")
                    
                except Exception as e:
                    print(f"   ❌ 批次 {i//batch_size + 1} 创建失败: {e}")
        
        print(f"🎉 关系创建完成! 总计: {created_count}")
    
    def demo_typed_queries(self):
        """演示基于类型标签的查询优势"""
        print("\n🔍 演示多标签查询功能:")
        
        try:
            with self.driver.session(database=self.database) as session:
                # 示例查询1：查找所有药材
                print("\n1. 查找所有药材 (Drug标签):")
                result = session.run("MATCH (d:Drug) RETURN d.name, d.type LIMIT 5")
                for record in result:
                    print(f"   - {record['d.name']} ({record['d.type']})")
                
                # 示例查询2：查找药材到方剂的关系
                print("\n2. 查找药材与方剂的治疗关系:")
                result = session.run("""
                    MATCH (d:Drug)-[r:RELATED_TO]-(f:Formula)
                    WHERE r.description CONTAINS '治疗' OR r.description CONTAINS '组成'
                    RETURN d.name, f.name, r.description LIMIT 3
                """)
                for record in result:
                    desc = record['r.description'][:50] + "..." if len(record['r.description']) > 50 else record['r.description']
                    print(f"   - {record['d.name']} ↔ {record['f.name']}: {desc}")
                
                # 示例查询3：疾病相关的治疗网络
                print("\n3. 查找疾病相关的治疗网络:")
                result = session.run("""
                    MATCH (disease:Disease)-[r1:RELATED_TO]-(formula:Formula)-[r2:RELATED_TO]-(drug:Drug)
                    RETURN disease.name, formula.name, drug.name LIMIT 3
                """)
                for record in result:
                    print(f"   - 疾病: {record['disease.name']} → 方剂: {record['formula.name']} → 药材: {record['drug.name']}")
                
                # 示例查询4：统计各类型实体的连接度
                print("\n4. 各类型实体的平均连接度:")
                labels = ['Drug', 'Formula', 'Disease', 'Symptom', 'Doctor', 'Efficacy']
                for label in labels:
                    result = session.run(f"""
                        MATCH (n:{label})
                        OPTIONAL MATCH (n)-[r]-()
                        WITH n, count(r) as degree
                        RETURN avg(degree) as avg_degree, count(n) as node_count
                    """)
                    record = result.single()
                    if record and record['node_count'] > 0:
                        print(f"   - {label}: 平均连接度 {record['avg_degree']:.2f}, 节点数 {record['node_count']}")
                
        except Exception as e:
            print(f"   ❌ 查询演示失败: {e}")
    
    def get_database_statistics(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        print("📊 获取数据库统计信息...")
        
        stats = {}
        
        with self.driver.session(database=self.database) as session:
            try:
                # 节点统计 - 统计所有实体类型
                result = session.run("MATCH (n) WHERE size(labels(n)) > 0 RETURN count(n) as node_count")
                stats['node_count'] = result.single()["node_count"]
                
                # 关系统计
                result = session.run("MATCH ()-[r:RELATED_TO]->() RETURN count(r) as rel_count")
                stats['relationship_count'] = result.single()["rel_count"]
                
                # 实体类型统计 - 手动统计各标签
                stats['entity_types'] = []
                label_result = session.run("CALL db.labels() YIELD label RETURN label")
                labels = [record["label"] for record in label_result]
                
                for label in labels:
                    count_result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                    record = count_result.single()
                    if record and record["count"] > 0:
                        stats['entity_types'].append((label, record["count"]))
                
                # 按数量排序
                stats['entity_types'].sort(key=lambda x: x[1], reverse=True)
                
                # 连接度统计 - 统计所有实体
                result = session.run("""
                    MATCH (n) WHERE size(labels(n)) > 0
                    RETURN avg(n.degree) as avg_degree, max(n.degree) as max_degree, min(n.degree) as min_degree
                """)
                degree_stats = result.single()
                stats['degree_stats'] = {
                    'average': round(degree_stats["avg_degree"], 2) if degree_stats["avg_degree"] else 0,
                    'maximum': degree_stats["max_degree"] or 0,
                    'minimum': degree_stats["min_degree"] or 0
                }
                
            except Exception as e:
                print(f"❌ 获取统计信息失败: {e}")
        
        return stats
    
    def print_statistics(self, stats: Dict[str, Any]):
        """打印统计信息"""
        print("\n" + "="*60)
        print("🏥 中医知识图谱数据库统计")
        print("="*60)
        
        print(f"📊 总节点数: {stats.get('node_count', 0):,}")
        print(f"🔗 总关系数: {stats.get('relationship_count', 0):,}")
        
        if 'degree_stats' in stats:
            degree = stats['degree_stats']
            print(f"📈 连接度统计:")
            print(f"   平均: {degree['average']}")
            print(f"   最大: {degree['maximum']}")
            print(f"   最小: {degree['minimum']}")
        
        if 'entity_types' in stats:
            print(f"\n🏷️  实体类型分布:")
            for entity_type, count in stats['entity_types']:
                print(f"   {entity_type}: {count:,}")
    
    def generate_browser_style(self) -> str:
        """生成Neo4j浏览器基础样式配置"""
        print("🎨 生成Neo4j浏览器基础样式...")
        
        # 基础样式
        basic_style = """node {
  color: #8DCC93;
  border-color: #5AA25F;
  text-color-internal: #FFFFFF;
  diameter: 40px;
  caption: {name};
}

relationship {
  color: #A5ABB6;
  shaft-width: 2px;
  font-size: 8px;
  padding: 3px;
  text-color-external: #000000;
  text-color-internal: #FFFFFF;
}"""
        
        # 保存到文件
        with open("neo4j_browser_style.grass", "w", encoding="utf-8") as f:
            f.write(basic_style)
        
        print("✅ 基础样式文件已保存到: neo4j_browser_style.grass")
        
        return basic_style
    
    def print_usage_instructions(self):
        """打印使用说明"""
        print("\n" + "="*60)
        print("🚀 Neo4j浏览器使用说明")
        print("="*60)
        
        print("1. 🌐 打开Neo4j浏览器:")
        print("   http://localhost:7474")
        print("   用户名: neo4j")
        print("   密码: password")
        
        print("\n2. 🎨 导入样式:")
        print("   在浏览器中执行: :style neo4j_browser_style.grass")
        print("   或者手动复制 neo4j_browser_style.grass 的内容到样式编辑器")
        
        print("\n3. 🔍 查询示例:")
        print("   查看所有实体类型:")
        print("   MATCH (n:Entity) RETURN DISTINCT n.type, count(n) ORDER BY count(n) DESC")
        
        print("\n   查看药材相关实体:")
        print("   MATCH (n:Entity) WHERE n.type CONTAINS '药材' OR n.type CONTAINS 'HERB' RETURN n LIMIT 20")
        
        print("\n   查看实体关系:")
        print("   MATCH (n:Entity)-[r:RELATED_TO]->(m:Entity) RETURN n, r, m LIMIT 50")
        
        print("\n   按权重查看重要关系:")
        print("   MATCH (n:Entity)-[r:RELATED_TO]->(m:Entity) WHERE r.weight > 0.5 RETURN n, r, m ORDER BY r.weight DESC LIMIT 20")
        
        print("\n4. 🏥 中医专用查询:")
        print("   查找疾病和症状关系:")
        print("   MATCH (d:Entity)-[r:RELATED_TO]-(s:Entity) WHERE d.type='疾病' AND s.type='症状' RETURN d, r, s LIMIT 30")
        
        print("\n   查找药材和功效关系:")
        print("   MATCH (h:Entity)-[r:RELATED_TO]-(e:Entity) WHERE h.type='药材' AND e.type='功效' RETURN h, r, e LIMIT 30")
        
        print("\n   查找方剂和医家关系:")
        print("   MATCH (p:Entity)-[r:RELATED_TO]-(d:Entity) WHERE p.type='方剂' AND d.type='医家' RETURN p, r, d LIMIT 30")
        
        print("\n   查看特定医家的相关实体:")
        print("   MATCH (d:Entity)-[r:RELATED_TO]-(n:Entity) WHERE d.type='医家' RETURN d, r, n LIMIT 40")

def main():
    """主函数"""
    print("🏥 中医知识图谱Neo4j数据库构建器")
    print("="*60)
    
    # 初始化构建器 - 可以指定数据库名称
    # 例如: builder = TCMNeo4jBuilder(database="tongue")
    builder = TCMNeo4jBuilder()
    
    try:
        # 0. 列出可用数据库 (可选)
        show_databases = input("是否显示可用数据库列表？(y/N): ").lower().strip()
        if show_databases == 'y':
            builder.list_databases()
        
        # 1. 连接数据库
        if not builder.connect():
            return
        
        # 2. 清空数据库 (可选)
        clear_db = input("是否清空现有数据库？(y/N): ").lower().strip()
        if clear_db == 'y':
            if not builder.clear_database():
                return
        
        # 3. 创建约束和索引
        builder.create_constraints_and_indexes()
        
        # 4. 加载实体数据
        entities_df = builder.load_entities()
        if entities_df.empty:
            print("❌ 无法加载实体数据")
            return
        
        # 5. 加载关系数据
        relationships_df = builder.load_relationships()
        if relationships_df.empty:
            print("❌ 无法加载关系数据")
            return
        
        # 6. 创建实体节点
        start_time = time.time()
        builder.create_entities(entities_df)
        entities_time = time.time() - start_time
        
        # 7. 创建关系
        start_time = time.time()
        builder.create_relationships(relationships_df)
        relationships_time = time.time() - start_time
        
        # 8. 获取并显示统计信息
        stats = builder.get_database_statistics()
        builder.print_statistics(stats)
        
        # 9. 生成浏览器样式
        builder.generate_browser_style()
        
        # 10. 显示使用说明
        builder.print_usage_instructions()
        
        print(f"\n⏱️  构建时间:")
        print(f"   实体创建: {entities_time:.2f}秒")
        print(f"   关系创建: {relationships_time:.2f}秒")
        print(f"   总计: {entities_time + relationships_time:.2f}秒")
        
        print(f"\n🎉 中医知识图谱构建完成!")
        
        # 演示多标签查询功能
        builder.demo_typed_queries()
        
    except KeyboardInterrupt:
        print("\n\n❌ 用户中断操作")
    except Exception as e:
        print(f"\n❌ 构建过程发生错误: {e}")
    finally:
        builder.close()

if __name__ == "__main__":
    main()
