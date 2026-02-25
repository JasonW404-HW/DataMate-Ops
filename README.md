# DataMate-Ops

DataMate 算子开发与调试框架。本项目采用单仓（Monorepo）结构，旨在为开发、测试和本地调试 DataMate 算子提供统一的环境和工具。未来所有新的算子都应在此框架下进行开发。

## 项目结构

```text
.
├── datamate/              # 核心框架代码（基础类、工具类等）
│   └── core/
│       └── base_op.py     # 算子基类 (Mapper)
├── <operator_name>/       # 具体算子目录（每个算子一个文件夹）
│   ├── process.py         # 算子核心逻辑
│   ├── metadata.yml       # 算子配置元数据
│   ├── requirements.txt   # 算子特定依赖
│   └── dataset/           # (可选) 本地测试数据集
├── main.py                # 本地调试入口
├── pyproject.toml         # 项目全局配置与依赖管理 (uv)
└── README.md              # 本文档
```

## 环境准备

本项目使用 [uv](https://github.com/astral-sh/uv) 进行依赖管理。

1. **安装 uv**: 请参考官方文档完成安装。
2. **初始化**: 在项目根目录执行以下命令，`uv` 会自动创建虚拟机环境并安装 `pyproject.toml` 中的基础依赖：
   ```bash
   uv sync
   ```

## 开发与调试流程

### 1. 开发新算子
- 在根目录下新建算子文件夹。
- 参照 `patho_sys_preprocess` 的结构编写 `process.py`, `metadata.yml` 和 `requirements.txt`。
- 算子类需继承 `datamate.core.base_op.Mapper` 并实现 `execute` 方法。

### 2. 局部调试 (main.py)
`main.py` 是统一的调试入口。当需要调试某个具体算子时：

1. **修改导入**: 在 `main.py` 顶部修改导入语句，指向你的算子：
   ```python
   from <operator_name>.process import <OperatorClass> as TestOperator
   ```
2. **配置参数**: 修改 `test_operator` 函数中的 `params` 字典，填入测试所需的配置。
3. **运行**:
   ```bash
   uv run main.py
   ```

## 发布指南
1. 确保算子目录内包含所有必要文件。
2. 将算子文件夹打包。
3. 在 DataMate 平台前端界面上传发布。

## 贡献建议
- 保持 `datamate/core` 的纯净，通用逻辑请抽象至此。
- 遵循统一的日志格式 `🟧🟧🟧 ... 🟦🟦🟦`，方便平台检索。