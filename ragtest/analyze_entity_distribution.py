#!/usr/bin/env python3
"""
分析实体类型分布
"""

try:
    import pandas as pd
    from pathlib import Path
    try:
        import yaml  # type: ignore
    except Exception:
        yaml = None
    
    # 尝试不同的文件名
    possible_files = [
        './output2/entities.parquet',
        './output2/entities.parquet'
    ]
    
    df = None
    for file_path in possible_files:
        try:
            df = pd.read_parquet(file_path)
            print(f"✅ 成功读取: {file_path}")
            break
        except Exception as e:
            print(f"❌ 无法读取 {file_path}: {e}")
    
    if df is None:
        print("❌ 无法读取实体文件")
        exit(1)
    
    print(f"\n📊 实体数据统计:")
    print("="*50)
    print(f"总实体数: {len(df):,}")
    
    # 从 settings.yaml 读取预定义实体类型
    def load_predefined_types() -> list[str]:
        base_dir = Path(__file__).resolve().parent
        settings_path = base_dir / 'settings.yaml'
        fallback = ['症状', '疾病', '体质', '药方', '舌象', '治疗方法', '文献']
        if yaml is None or not settings_path.exists():
            print("⚠️ 未找到PyYAML或settings.yaml，使用默认预定义类型")
            return fallback
        try:
            with settings_path.open('r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f) or {}
            types = ((cfg.get('extract_graph') or {}).get('entity_types'))
            if isinstance(types, list) and types:
                return [str(x).strip() for x in types if str(x).strip()]
            return fallback
        except Exception as e:
            print(f"⚠️ 读取settings.yaml失败，使用默认预定义类型: {e}")
            return fallback

    predefined_types = load_predefined_types()
    print(f"预定义类型(来自settings.yaml): {predefined_types}")

    if 'type' in df.columns:
        print(f"\n🏷️ 实体类型分布:")
        print("-" * 30)
        
        series_type = pd.Series(list(df['type']))
        type_counts = pd.Series(list(series_type)).value_counts()
        total = len(df)
        
        for entity_type, count in type_counts.head(20).items():
            percentage = count / total * 100
            if entity_type == '':
                print(f"{'[空类型]':<15}: {count:>6,} ({percentage:>5.1f}%)")
            else:
                print(f"{entity_type:<15}: {count:>6,} ({percentage:>5.1f}%)")
        
        # 统计问题实体
        empty_count = int((series_type == '').sum())
        predefined_count = int(series_type.isin(predefined_types).sum())
        other_count = total - empty_count - predefined_count
        
        print(f"\n📈 分类统计:")
        print("-" * 30)
        print(f"预定义类型实体: {predefined_count:,} ({predefined_count/total*100:.1f}%)")
        print(f"其他类型实体:   {other_count:,} ({other_count/total*100:.1f}%)")
        print(f"空类型实体:     {empty_count:,} ({empty_count/total*100:.1f}%)")
        
        if other_count > 0:
            print(f"\n🔍 非预定义类型:")
            print("-" * 30)
            mask = ~series_type.isin(list(predefined_types) + [''])
            other_types = pd.Series(list(series_type[mask])).value_counts()
            for otype, count in other_types.head(10).items():
                print(f"{otype:<15}: {count:>6,}")
    
    # 检查实体名为空字符串的实体
    if 'title' in df.columns:
        print(f"\n⚠️ 实体名为空的实体:")
        print("-" * 50)
        empty_name_mask = (df['title'] == '') | (df['title'].isna())
        empty_name_entities = df[empty_name_mask]
        
        if len(empty_name_entities) > 0:
            print(f"发现 {len(empty_name_entities)} 个实体名为空的实体:")
            for idx, row in empty_name_entities.head(10).iterrows():
                entity_type = row.get('type', 'Unknown')
                entity_id = row.get('id', idx)
                description = row.get('description', '')[:100] + ('...' if len(str(row.get('description', ''))) > 100 else '')
                print(f"  ID: {entity_id} | 类型: {entity_type} | 描述: {description}")
            if len(empty_name_entities) > 10:
                print(f"  ... 还有 {len(empty_name_entities) - 10} 个实体")
        else:
            print("✅ 未发现实体名为空的实体")
    
    # 检查描述为空字符串的实体
    if 'description' in df.columns:
        print(f"\n⚠️ 描述为空的实体:")
        print("-" * 50)
        empty_desc_mask = (df['description'] == '') | (df['description'].isna())
        empty_desc_entities = df[empty_desc_mask]
        
        if len(empty_desc_entities) > 0:
            print(f"发现 {len(empty_desc_entities)} 个描述为空的实体:")
            for idx, row in empty_desc_entities.head(10).iterrows():
                entity_name = row.get('title', 'Unknown')
                entity_type = row.get('type', 'Unknown')
                entity_id = row.get('id', idx)
                print(f"  ID: {entity_id} | 名称: {entity_name} | 类型: {entity_type}")
            if len(empty_desc_entities) > 10:
                print(f"  ... 还有 {len(empty_desc_entities) - 10} 个实体")
        else:
            print("✅ 未发现描述为空的实体")
                
    else:
        print("❌ 没有找到type列")
        
except ImportError:
    print("❌ 需要安装pandas: pip install pandas")
except Exception as e:
    print(f"❌ 分析失败: {e}")