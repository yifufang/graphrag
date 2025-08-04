#!/usr/bin/env python3
"""
安装neomodel和相关依赖的脚本
"""

import subprocess
import sys

def install_packages():
    """安装必要的Python包"""
    packages = [
        "neomodel>=5.0.0",
        "neo4j>=5.0.0", 
        "pandas",
        "pytz"  # neomodel的依赖
    ]
    
    print("🔧 安装neomodel和相关依赖包...")
    print(f"📦 将要安装的包: {', '.join(packages)}")
    
    try:
        for package in packages:
            print(f"📥 安装 {package}...")
            subprocess.check_call([
                sys.executable, "-m", "pip", "install", 
                "--quiet", "--upgrade", package
            ])
            print(f"✅ {package} 安装成功")
        
        print("\n🎉 所有依赖包安装完成!")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ 安装失败: {e}")
        print("💡 请确保:")
        print("   1. 你有网络连接")
        print("   2. pip工具正常工作")
        print("   3. 你有安装权限")
        return False

def verify_installation():
    """验证安装是否成功"""
    print("\n🔍 验证安装...")
    
    try:
        import neomodel
        print(f"✅ neomodel 版本: {neomodel.__version__}")
        
        from neo4j import __version__ as neo4j_version
        print(f"✅ neo4j-driver 版本: {neo4j_version}")
        
        import pandas as pd
        print(f"✅ pandas 版本: {pd.__version__}")
        
        print("🎉 所有包验证成功!")
        return True
        
    except ImportError as e:
        print(f"❌ 导入失败: {e}")
        return False

def main():
    """主函数"""
    print("🚀 neomodel依赖安装工具")
    print("="*50)
    
    # 安装包
    if not install_packages():
        return 1
    
    # 验证安装
    if not verify_installation():
        return 1
    
    print("\n" + "="*50)
    print("✨ 准备就绪!")
    print("现在可以运行: python neomodel_query.py")
    print("="*50)
    
    return 0

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n❌ 用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 安装过程出错: {e}")
        sys.exit(1)