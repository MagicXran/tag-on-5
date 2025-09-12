#!/usr/bin/env python3
"""
RMS标签导入处理系统 - GUI启动脚本
"""

import sys
import os

# 添加当前目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

def main():
    """主启动函数"""
    try:
        from gui_main import main as gui_main
        print("启动RMS标签导入处理系统GUI界面...")
        gui_main()
    except ImportError as e:
        print(f"导入错误: {e}")
        print("请确保已安装所有依赖包:")
        print("pip install -r requirements.txt")
        sys.exit(1)
    except Exception as e:
        print(f"启动失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()