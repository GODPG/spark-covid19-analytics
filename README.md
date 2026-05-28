\# 🦠 基于 Apache Spark 的美国新冠疫情多维数据挖掘与可视化推演

> \*\*大数据处理与分析课程设计项目\*\*



!\[Spark](https://img.shields.io/badge/Spark-3.1.1-e25a1c.svg) 

!\[Hadoop](https://img.shields.io/badge/Hadoop-3.3.0-f5d622.svg)

!\[Python](https://img.shields.io/badge/Python-3.8+-blue.svg)

!\[PyEcharts](https://img.shields.io/badge/PyEcharts-1.9.0+-red.svg)

!\[License](https://img.shields.io/badge/License-MIT-green.svg)



本项目基于纽约时报开源的美国 COVID-19 历史追踪数据集，构建了以 \*\*Hadoop HDFS\*\* 为底层存储、\*\*Apache Spark\*\* 为核心内存计算引擎、\*\*PyEcharts\*\* 为前端渲染的大数据分析流水线。项目自底向上执行了数据清洗、特征衍生、时序平滑、传播动力学建模以及 K-Means 机器智能聚类，共计实现了 20 项高阶数据科学分析需求。



\## 📑 核心特性与架构



系统采用高内聚、低耦合的三层批处理架构：



1\. \*\*分布式存储与调度 (HDFS / YARN)：\*\* 应对百万级时空异构数据，解决单机 I/O 瓶颈。

2\. \*\*Spark 多维计算层 (Core / SQL / MLlib)：\*\*

&#x20;  - 摒弃昂贵的全局自连接 (Self-Join)，大量采用 Spark `Window` 开窗函数与 `lag` 算子实现时序指标的滑动计算与降噪。

&#x20;  - 引入 K-Means 算法在双对数坐标系下进行多维特征的无监督风险定级。

3\. \*\*动态可视化呈现 (PyEcharts)：\*\* 数据结果轻量化 JSON 落盘，驱动前端动态时间轴、南丁格尔玫瑰图、地理映射飞线图等交互式渲染。



\## 🛠️ 环境依赖



\- 操作系统：Ubuntu 20.04

\- 核心组件：Hadoop 3.3.0, Spark 3.1.1 (Standalone 或 YARN 模式)

\- 语言环境：Python 3.8+ 

\- 核心依赖库：`pyspark`, `pyecharts`



\## 🚀 快速启动



\*\*1. 准备数据\*\*

将原始数据集 `us-counties.txt` 上传至 HDFS 指定目录：

```bash

hdfs dfs -mkdir -p /user/hadoop-wei/

hdfs dfs -put data/us-counties.txt /user/hadoop-wei/

