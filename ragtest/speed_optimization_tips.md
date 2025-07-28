# GraphRAG 大文本量处理加速指南

## 🔧 硬件优化建议

### 1. 系统资源
- **CPU**: 确保有足够的CPU核心数（建议8核以上）
- **内存**: 至少16GB RAM，推荐32GB+
- **存储**: 使用SSD硬盘，避免机械硬盘瓶颈

### 2. 网络优化
- 确保到API端点的网络稳定
- 考虑使用本地部署的模型以减少网络延迟

## 📁 数据预处理建议

### 1. 文件分批处理
```bash
# 将大文件分割为较小的批次
mkdir -p input/batch1 input/batch2 input/batch3
# 每批处理完后再处理下一批
```

### 2. 文件预清理
- 移除不必要的空行和格式字符
- 统一文本编码为UTF-8
- 预先过滤掉质量较低的文本

## ⚡ 运行时优化

### 1. 分阶段处理
```bash
# 第一阶段：基础索引构建
python -m graphrag.index --config ragtest/settings.yaml

# 检查结果后再启用更多功能
```

### 2. 缓存利用
- 保持cache目录，避免重复处理
- 定期清理过期缓存以节省空间

### 3. 监控资源使用
```bash
# Windows系统监控
tasklist /fi "imagename eq python.exe"
wmic cpu get loadpercentage
```

## 🎯 渐进式优化策略

### 阶段1：快速原型
```yaml
# 最小配置，快速验证
extract_claims:
  enabled: false
embed_graph:
  enabled: false
chunks:
  size: 1500  # 更大的块
```

### 阶段2：质量平衡
```yaml
# 在速度和质量间平衡
extract_claims:
  enabled: true
  max_gleanings: 1
```

### 阶段3：完整处理
```yaml
# 启用所有功能进行最终处理
extract_claims:
  enabled: true
  max_gleanings: 2
embed_graph:
  enabled: true
```

## 🔍 性能监控指标

### 关键指标
- 每分钟处理的文本块数
- API调用成功率
- 内存使用峰值
- 磁盘I/O负载

### 调优建议
1. 如果内存不足，减少`concurrent_requests`
2. 如果API频繁超时，降低`requests_per_minute`
3. 如果磁盘I/O成为瓶颈，考虑使用RAM磁盘

## 🚨 常见问题解决

### API限流
- 适当降低`requests_per_minute`
- 增加`request_timeout`

### 内存不足
- 减少`batch_size`
- 降低`concurrent_requests`

### 处理卡住
- 检查网络连接
- 查看logs目录下的错误日志
- 清理cache后重新开始 