#!/usr/bin/env python3
"""
Simple Embedding Model Comparison Script

A minimal version that only requires the requests library.
Compares qwen3-embedding-new vs bge-m3 embedding models.

Tests include both English and Chinese (中文) text to evaluate:
- Response time (响应时间)
- Semantic similarity quality (语义相似度质量)
- Cross-language performance (跨语言性能)
- Traditional Chinese Medicine terms (中医术语)
"""

import requests
import json
import time
import math
from typing import List, Dict, Tuple

def dot_product(v1: List[float], v2: List[float]) -> float:
    """Calculate dot product of two vectors"""
    return sum(x * y for x, y in zip(v1, v2))

def magnitude(vector: List[float]) -> float:
    """Calculate magnitude of a vector"""
    return math.sqrt(sum(x * x for x in vector))

def cosine_similarity(v1: List[float], v2: List[float]) -> float:
    """Calculate cosine similarity between two vectors"""
    dot_prod = dot_product(v1, v2)
    mag1 = magnitude(v1)
    mag2 = magnitude(v2)
    
    if mag1 == 0 or mag2 == 0:
        return 0
    
    return dot_prod / (mag1 * mag2)

def get_embedding(text: str, model_url: str, model_name: str) -> Tuple[List[float], float] | Tuple[None, None]:
    """Get embedding for text using specified model"""
    headers = {"Content-Type": "application/json"}
    payload = {
        "input": text,
        "model": model_name
    }
    
    start_time = time.time()
    try:
        response = requests.post(model_url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        
        end_time = time.time()
        response_time = end_time - start_time
        
        data = response.json()
        embedding = data["data"][0]["embedding"]
        
        return embedding, response_time
    
    except Exception as e:
        print(f"Error: {e}")
        return None, None

def test_models():
    """Test and compare both embedding models"""
    
    # Model configurations
    models = {
        "Qwen3-Embedding-New": {
            "url": "http://43.143.178.100:8000/model4/v1/embeddings",
            "model": "qwen3-embedding-new"
        },
        "BGE-M3": {
            "url": "http://43.143.178.100:8000/model5/v1/embeddings", 
            "model": "bge-m3"
        }
    }
    
    # Test cases - Including Chinese text
    test_cases = [
        {
            "name": "Similar Sentences (English)",
            "texts": [
                "The cat is sleeping on the couch.",
                "A cat is resting on the sofa.",
                "The dog is playing in the yard."
            ]
        },
        {
            "name": "相似句子 (中文)",
            "texts": [
                "猫咪在沙发上睡觉。",
                "一只猫在沙发上休息。",
                "小狗在院子里玩耍。"
            ]
        },
        {
            "name": "Technical Terms (English)",
            "texts": [
                "Machine learning algorithms process data.",
                "AI models analyze information patterns.",
                "The weather is sunny today."
            ]
        },
        {
            "name": "技术术语 (中文)",
            "texts": [
                "机器学习算法处理数据。",
                "人工智能模型分析信息模式。",
                "今天天气很晴朗。"
            ]
        },
        {
            "name": "Medical Context (English)",
            "texts": [
                "The patient has high blood pressure.",
                "Hypertension affects cardiovascular health.",
                "Python is a programming language."
            ]
        },
        {
            "name": "医学语境 (中文)",
            "texts": [
                "患者有高血压。",
                "高血压影响心血管健康。",
                "Python是一种编程语言。"
            ]
        },
        {
            "name": "中医术语",
            "texts": [
                "气血两虚，需要补益气血。",
                "气血不足，应当滋补气血。",
                "肝火旺盛，清热解毒。"
            ]
        },
        {
            "name": "Mixed Language",
            "texts": [
                "Machine learning 机器学习",
                "Artificial Intelligence 人工智能",
                "Deep learning 深度学习"
            ]
        }
    ]
    
    results = {}
    
    print("=" * 60)
    print("SIMPLE EMBEDDING MODEL COMPARISON / 简单嵌入模型对比")
    print("=" * 60)
    print("Testing both English and Chinese text / 测试英文和中文文本")
    
    # Test each model
    for model_name, config in models.items():
        print(f"\n--- Testing {model_name} ---")
        model_results = {}
        total_time = 0
        total_requests = 0
        
        for test_case in test_cases:
            print(f"\nTest Case: {test_case['name']}")
            embeddings = []
            response_times = []
            
            # Get embeddings for all texts
            for i, text in enumerate(test_case['texts']):
                print(f"  Processing text {i+1}...", end="")
                
                embedding, resp_time = get_embedding(text, config["url"], config["model"])
                
                if embedding is not None and resp_time is not None:
                    embeddings.append(embedding)
                    response_times.append(resp_time)
                    total_time += resp_time
                    total_requests += 1
                    print(f" {resp_time:.3f}s")
                else:
                    print(" FAILED")
                    break
            
            if len(embeddings) == len(test_case['texts']):
                # Calculate similarities
                similarities = []
                for i in range(len(embeddings)):
                    for j in range(i+1, len(embeddings)):
                        sim = cosine_similarity(embeddings[i], embeddings[j])
                        similarities.append(sim)
                        print(f"    Similarity between text {i+1} and {j+1}: {sim:.4f}")
                
                avg_similarity = sum(similarities) / len(similarities)
                avg_response_time = sum(response_times) / len(response_times)
                
                model_results[test_case['name']] = {
                    'avg_similarity': avg_similarity,
                    'avg_response_time': avg_response_time,
                    'similarities': similarities
                }
                
                print(f"    Average similarity: {avg_similarity:.4f}")
                print(f"    Average response time: {avg_response_time:.3f}s")
            else:
                print(f"    Failed to get all embeddings for {test_case['name']}")
        
        model_results['overall_avg_time'] = total_time / total_requests if total_requests > 0 else 0
        results[model_name] = model_results
    
    # Compare results
    print("\n" + "=" * 60)
    print("COMPARISON SUMMARY")
    print("=" * 60)
    
    if len(results) == 2:
        model_names = list(results.keys())
        model1, model2 = model_names[0], model_names[1]
        
        print(f"\n{model1} vs {model2}:")
        print("-" * 40)
        
        # Overall performance metrics
        avg_time1 = results[model1].get('overall_avg_time', 0)
        avg_time2 = results[model2].get('overall_avg_time', 0)
        
        print(f"Average Response Time:")
        print(f"  {model1}: {avg_time1:.3f}s")
        print(f"  {model2}: {avg_time2:.3f}s")
        print(f"  Winner: {model1 if avg_time1 < avg_time2 else model2} (faster)")
        
        # Test case comparisons
        for test_case in test_cases:
            case_name = test_case['name']
            if case_name in results[model1] and case_name in results[model2]:
                sim1 = results[model1][case_name]['avg_similarity']
                sim2 = results[model2][case_name]['avg_similarity']
                time1 = results[model1][case_name]['avg_response_time']
                time2 = results[model2][case_name]['avg_response_time']
                
                print(f"\n{case_name}:")
                print(f"  Similarity - {model1}: {sim1:.4f}, {model2}: {sim2:.4f}")
                print(f"  Response Time - {model1}: {time1:.3f}s, {model2}: {time2:.3f}s")
                print(f"  Better similarity: {model1 if sim1 > sim2 else model2}")
                print(f"  Faster: {model1 if time1 < time2 else model2}")
        
        # Overall recommendation
        print(f"\n" + "=" * 40)
        print("OVERALL RECOMMENDATION")
        print("=" * 40)
        
        # Simple scoring: 50% weight on speed, 50% on similarity quality
        score1 = 0
        score2 = 0
        
        # Speed score (inverse of time, normalized)
        if avg_time1 > 0 and avg_time2 > 0:
            speed_score1 = 1 / avg_time1
            speed_score2 = 1 / avg_time2
            total_speed = speed_score1 + speed_score2
            score1 += (speed_score1 / total_speed) * 0.5
            score2 += (speed_score2 / total_speed) * 0.5
        
        # Similarity score
        similarity_scores1 = []
        similarity_scores2 = []
        
        for test_case in test_cases:
            case_name = test_case['name']
            if case_name in results[model1] and case_name in results[model2]:
                similarity_scores1.append(results[model1][case_name]['avg_similarity'])
                similarity_scores2.append(results[model2][case_name]['avg_similarity'])
        
        if similarity_scores1 and similarity_scores2:
            avg_sim1 = sum(similarity_scores1) / len(similarity_scores1)
            avg_sim2 = sum(similarity_scores2) / len(similarity_scores2)
            total_sim = avg_sim1 + avg_sim2
            
            if total_sim > 0:
                score1 += (avg_sim1 / total_sim) * 0.5
                score2 += (avg_sim2 / total_sim) * 0.5
        
        print(f"Final Scores:")
        print(f"  {model1}: {score1:.3f}")
        print(f"  {model2}: {score2:.3f}")
        print(f"\nRecommended Model: {model1 if score1 > score2 else model2}")
        

def main():
    """Main function / 主函数"""
    print("Testing embedding models with Chinese and English text...")
    print("测试中英文嵌入模型...")
    print("Make sure the API endpoints are accessible.")
    print("请确保API端点可访问。")
    
    try:
        test_models()
    except KeyboardInterrupt:
        print("\nTest interrupted by user. / 测试被用户中断。")
    except Exception as e:
        print(f"Error during testing: {e}")
        print(f"测试过程中出错: {e}")

if __name__ == "__main__":
    main()