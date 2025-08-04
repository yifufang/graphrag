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
from typing import Dict, List, Any, Optional
import warnings
warnings.filterwarnings('ignore')

class TCMNeo4jBuilder:
    """中医知识图谱Neo4j构建器"""
    
    def __init__(self, uri: str = "neo4j://localhost", username: str = "neo4j", password: str = "password"):
        """初始化Neo4j连接"""
        self.uri = uri
        self.username = username
        self.password = password
        self.driver = None
        
        # 中医实体类型颜色配置 - 匹配settings.yaml中定义的6个核心实体类型
        self.entity_colors = {
            # 核心中医实体类型 (与settings.yaml中entity_types匹配)
            "药材": "#4CAF50",      # 绿色 - 药材
            "方剂": "#2196F3",      # 蓝色 - 方剂
            "疾病": "#F44336",      # 红色 - 疾病
            "症状": "#FF9800",      # 橙色 - 症状
            "医家": "#673AB7",      # 深紫色 - 医家
            "功效": "#9C27B0",      # 紫色 - 功效
            
            # 英文对应 (兼容性)
            "HERB": "#4CAF50",         # 药材
            "PRESCRIPTION": "#2196F3",  # 方剂
            "DISEASE": "#F44336",       # 疾病
            "SYMPTOM": "#FF9800",       # 症状
            "DOCTOR": "#673AB7",        # 医家
            "EFFICACY": "#9C27B0",      # 功效
            
            # GraphRAG默认类型
            "PERSON": "#795548",       # 棕色 - 人物
            "ORGANIZATION": "#3F51B5", # 靛蓝色 - 组织
            "GEO": "#009688",          # 青色 - 地理位置
            "EVENT": "#FF5722",        # 深橙色 - 事件
            
            # 其他可能的中医相关类型
            "治疗": "#E91E63",      # 粉红色 - 治疗方法
            "经络": "#607D8B",      # 灰蓝色 - 经络
            "穴位": "#795548",      # 棕色 - 穴位
            "体质": "#CDDC39",      # 青绿色 - 体质
            
            # 默认颜色
            "DEFAULT": "#616161"       # 灰色
        }
    
    def connect(self) -> bool:
        """连接到Neo4j数据库"""
        try:
            print("🔌 连接到Neo4j数据库...")
            self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
            
            # 测试连接
            with self.driver.session() as session:
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
    
    def clear_database(self) -> bool:
        """清空数据库"""
        try:
            print("🗑️  清空数据库...")
            if not self.driver:
                print("❌ 数据库连接未建立")
                return False
            with self.driver.session() as session:
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
        
        constraints_and_indexes = [
            # 实体约束和索引
            "CREATE CONSTRAINT entity_id_unique IF NOT EXISTS FOR (e:Entity) REQUIRE e.id IS UNIQUE",
            "CREATE INDEX entity_name_index IF NOT EXISTS FOR (e:Entity) ON (e.name)",
            "CREATE INDEX entity_type_index IF NOT EXISTS FOR (e:Entity) ON (e.type)",
            
            # 关系索引
            "CREATE INDEX relationship_weight_index IF NOT EXISTS FOR ()-[r:RELATED_TO]-() ON (r.weight)",
            "CREATE INDEX relationship_rank_index IF NOT EXISTS FOR ()-[r:RELATED_TO]-() ON (r.rank)"
        ]
        
        with self.driver.session() as session:
            for cmd in constraints_and_indexes:
                try:
                    session.run(cmd)
                    print(f"   ✅ {cmd.split()[1]} 创建成功")
                except Exception as e:
                    print(f"   ⚠️  {cmd}: {e}")
    
    def load_entities(self, entities_file: str = "./output/entities.parquet") -> pd.DataFrame:
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
            
            return entities_df
            
        except Exception as e:
            print(f"❌ 加载实体数据失败: {e}")
            return pd.DataFrame()
    
    def load_relationships(self, relationships_file: str = "./output/relationships.parquet") -> pd.DataFrame:
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
    
    def get_entity_color(self, entity_type: str) -> str:
        """根据实体类型获取颜色"""
        if not entity_type:
            return self.entity_colors["DEFAULT"]
        
        # 清理类型字符串
        clean_type = str(entity_type).strip().strip('"').upper()
        
        # 尝试精确匹配
        if clean_type in self.entity_colors:
            return self.entity_colors[clean_type]
        
        # 尝试包含匹配
        for type_key, color in self.entity_colors.items():
            if type_key.upper() in clean_type or clean_type in type_key.upper():
                return color
        
        return self.entity_colors["DEFAULT"]
    
    def create_entities(self, entities_df: pd.DataFrame, batch_size: int = 1000):
        """批量创建实体节点"""
        print(f"🏗️  创建实体节点 (批次大小: {batch_size})...")
        
        if not self.driver:
            print("❌ 数据库连接未建立")
            return
        
        total_entities = len(entities_df)
        created_count = 0
        
        with self.driver.session() as session:
            for i in range(0, total_entities, batch_size):
                batch = entities_df.iloc[i:i+batch_size]
                
                # 准备批次数据
                entities_data = []
                for _, row in batch.iterrows():
                    entity_type = str(row.get('type', '')).strip().strip('"') if pd.notna(row.get('type')) else ''
                    color = self.get_entity_color(entity_type)
                    
                    entity_data = {
                        'id': str(row.get('id', '')),
                        'name': str(row.get('title', row.get('name', ''))).strip().strip('"'),
                        'type': entity_type,
                        'description': str(row.get('description', ''))[:1000] if pd.notna(row.get('description')) else '',
                        'human_readable_id': int(row.get('human_readable_id', 0)) if pd.notna(row.get('human_readable_id')) else 0,
                        'degree': int(row.get('degree', 0)) if pd.notna(row.get('degree')) else 0,
                        'color': color
                    }
                    entities_data.append(entity_data)
                
                # 批量插入
                try:
                    session.run("""
                        UNWIND $entities as entity
                        CREATE (e:Entity)
                        SET e.id = entity.id,
                            e.name = entity.name,
                            e.type = entity.type,
                            e.description = entity.description,
                            e.human_readable_id = entity.human_readable_id,
                            e.degree = entity.degree,
                            e.color = entity.color
                    """, entities=entities_data)
                    
                    created_count += len(batch)
                    print(f"   ✅ 已创建 {created_count}/{total_entities} 个实体 ({created_count/total_entities*100:.1f}%)")
                    
                except Exception as e:
                    print(f"   ❌ 批次 {i//batch_size + 1} 创建失败: {e}")
        
        print(f"🎉 实体创建完成! 总计: {created_count}")
    
    def create_relationships(self, relationships_df: pd.DataFrame, batch_size: int = 1000):
        """批量创建关系"""
        print(f"🔗 创建关系 (批次大小: {batch_size})...")
        
        total_relationships = len(relationships_df)
        created_count = 0
        
        with self.driver.session() as session:
            for i in range(0, total_relationships, batch_size):
                batch = relationships_df.iloc[i:i+batch_size]
                
                # 准备批次数据
                relationships_data = []
                for _, row in batch.iterrows():
                    relationship_data = {
                        'source_name': str(row.get('source', '')).strip().strip('"'),
                        'target_name': str(row.get('target', '')).strip().strip('"'),
                        'id': str(row.get('id', '')),
                        'description': str(row.get('description', ''))[:500] if pd.notna(row.get('description')) else '',
                        'weight': float(row.get('weight', 1.0)) if pd.notna(row.get('weight')) else 1.0,
                        'rank': int(row.get('rank', 0)) if pd.notna(row.get('rank')) else 0
                    }
                    relationships_data.append(relationship_data)
                
                # 批量插入关系
                try:
                    session.run("""
                        UNWIND $relationships as rel
                        MATCH (source:Entity {name: rel.source_name})
                        MATCH (target:Entity {name: rel.target_name})
                        CREATE (source)-[r:RELATED_TO]->(target)
                        SET r.id = rel.id,
                            r.description = rel.description,
                            r.weight = rel.weight,
                            r.rank = rel.rank
                    """, relationships=relationships_data)
                    
                    created_count += len(batch)
                    print(f"   ✅ 已创建 {created_count}/{total_relationships} 个关系 ({created_count/total_relationships*100:.1f}%)")
                    
                except Exception as e:
                    print(f"   ❌ 批次 {i//batch_size + 1} 创建失败: {e}")
        
        print(f"🎉 关系创建完成! 总计: {created_count}")
    
    def get_database_statistics(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        print("📊 获取数据库统计信息...")
        
        stats = {}
        
        with self.driver.session() as session:
            try:
                # 节点统计
                result = session.run("MATCH (n:Entity) RETURN count(n) as node_count")
                stats['node_count'] = result.single()["node_count"]
                
                # 关系统计
                result = session.run("MATCH ()-[r:RELATED_TO]->() RETURN count(r) as rel_count")
                stats['relationship_count'] = result.single()["rel_count"]
                
                # 实体类型统计
                result = session.run("""
                    MATCH (n:Entity) 
                    RETURN n.type as type, count(n) as count, n.color as color
                    ORDER BY count DESC
                """)
                stats['entity_types'] = [(record["type"], record["count"], record["color"]) for record in result]
                
                # 连接度统计
                result = session.run("""
                    MATCH (n:Entity)
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
            for entity_type, count, color in stats['entity_types']:
                print(f"   {entity_type}: {count:,} (颜色: {color})")
    
    def generate_browser_style(self) -> str:
        """生成Neo4j浏览器样式配置"""
        print("🎨 生成Neo4j浏览器样式...")
        
        # 构建样式字符串
        styles = []
        
        # 为每种实体类型生成样式
        for entity_type, color in self.entity_colors.items():
            if entity_type != "DEFAULT":
                style = f"""
node[type="{entity_type}"] {{
  color: {color};
  border-color: {color};
  text-color-internal: #FFFFFF;
  diameter: 50px;
  caption: {{name}};
}}"""
                styles.append(style)
        
        # 默认样式
        default_style = f"""
node {{
  color: {self.entity_colors['DEFAULT']};
  border-color: {self.entity_colors['DEFAULT']};
  text-color-internal: #FFFFFF;
  diameter: 40px;
  caption: {{name}};
}}

relationship {{
  color: #A5ABB6;
  shaft-width: 2px;
  font-size: 8px;
  padding: 3px;
  text-color-external: #000000;
  text-color-internal: #FFFFFF;
}}"""
        
        styles.append(default_style)
        
        full_style = "\n".join(styles)
        
        # 保存到文件
        with open("neo4j_browser_style.grass", "w", encoding="utf-8") as f:
            f.write(full_style)
        
        print("✅ 样式文件已保存到: neo4j_browser_style.grass")
        
        return full_style
    
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
    
    # 初始化构建器
    builder = TCMNeo4jBuilder()
    
    try:
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
        
    except KeyboardInterrupt:
        print("\n\n❌ 用户中断操作")
    except Exception as e:
        print(f"\n❌ 构建过程发生错误: {e}")
    finally:
        builder.close()

if __name__ == "__main__":
    main()
