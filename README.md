# 小红书数据采集与分析平台

项目当前完全以 `CrawlerData` 为数据标准，`MediaCrawler-main` 只保留为独立爬虫模块，不参与分析平台运行。系统会先在后台完成表单兼容、合并、清洗和去重，再把重点放到多页分析看板、可视化、舆情检测、文本洞察、指标关联、聚类和轻量机器学习分析上。

## 当前能力

- 只读取 `CrawlerData/*.csv`，不再依赖公开数据集接口
- 同表头文件先合并，再统一标准化与去重
- 自动跳过内嵌表头行、异常字段和无法关联的孤立评论
- 前端改为多页切换结构，提供导航栏和页内索引，不再依赖整页长滚动
- 首页与主页面聚焦分析结果，数据清洗信息被收纳到最后一页“数据资产”
- 支持情绪分布、风险关键词、内容质量分层、指标相关性、聚类画像、简单线性模型特征影响力等图表

## 页面结构

- `总览`：样本规模、发布趋势、地域互动、头部作者、高质量样本
- `舆情监测`：情绪分布、情绪时序、风险关键词、争议笔记、高热负向评论
- `内容分析`：关键词、标签、质量分层、正文长度与互动关系、高质量内容样本
- `关联挖掘`：相关性热力图、点赞评论收藏关系、特征影响力、强相关指标对
- `聚类洞察`：无监督聚类散点、聚类雷达、聚类摘要
- `数据资产`：清洗结果、异常提示、表单格式兼容情况

## 目录说明

```text
.
├─CrawlerData/                      # 原始采集数据
├─MediaCrawler-main/                # 已完成的爬虫模块，分析系统不直接调用
├─analysis_system/
│  ├─app.py                         # Flask 入口与 API
│  ├─analysis_service.py            # 分析缓存与聚合服务
│  ├─dashboard/
│  │  └─index.html                  # 多页导航分析看板
│  ├─analyzer/
│  │  ├─common.py                   # 通用清洗工具
│  │  ├─data_loader.py              # 表单合并、预处理、去重、导出
│  │  ├─stats_analyzer.py           # 统计分析
│  │  ├─text_analyzer.py            # 文本分析
│  │  ├─trend_analyzer.py           # 趋势分析
│  │  └─mining_analyzer.py          # 舆情、关联、聚类、机器学习挖掘
│  ├─data/
│  │  └─processed/                  # 运行后生成的标准化产物
│  ├─start.bat                      # Windows 启动脚本
│  └─run_test.py                    # 本地烟测脚本
└─README.md
```

## 启动

```powershell
cd analysis_system
pip install -r requirements.txt
python app.py
```

访问 `http://127.0.0.1:8080`

Windows 也可以直接运行 `analysis_system\start.bat`。

## 运行产物

首次启动会自动生成：

- `analysis_system/data/processed/notes_cleaned.csv`
- `analysis_system/data/processed/comments_cleaned.csv`
- `analysis_system/data/processed/preprocess_report.json`

## API

- `GET /api/health`
- `GET /api/dashboard`
- `GET /api/overview`
- `GET /api/opinion`
- `GET /api/content`
- `GET /api/relationship`
- `GET /api/clusters`
- `GET /api/summary`
- `GET /api/quality`
- `GET /api/stats`
- `GET /api/text`
- `GET /api/trend`
- `GET /api/notes`
- `GET /api/comments`
- `POST /api/rebuild`

## 清洗规则

- 按表头分组，同格式文件优先合并
- `note_id` / `comment_id` 作为主键去重
- 文本字段优先保留更完整的记录
- 点赞、收藏、评论、分享等数值字段按更可信的高值保留
- 时间字段按发布时间最早、修改时间最新的原则收敛
- 标签、图片、来源文件会做去重合并
- 内嵌表头行和明显异常行会在预处理阶段过滤，避免污染分析结果

## 烟测

```powershell
cd analysis_system
python run_test.py
```

烟测会直接请求核心接口，检查清洗链路、分析缓存和多页看板数据源是否正常。
