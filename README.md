# 第二部分 A 题最终代码

本目录整理自实验实际运行成功的最终版本。

- `Dockerfile`：基于教师提供的 `pyspark:v9` 镜像，加入豆瓣数据集和三个 PySpark 脚本。
- `douban_clean.py`：A-1 数据加载、Schema、前 5 行、缺失比例、两种清洗策略和基本统计。
- `douban_analyze.py`：A-2 四类 Spark SQL 查询：GROUP BY、Top-N、时间趋势、JOIN + WINDOW。
- `douban_benchmark.py`：A-3 Pandas 与 PySpark 1/2 executor 性能对比。
- `wordcount.yaml`：A-0 WordCount 示例 SparkApplication。
- `douban-*.yaml`：A-1/A-2/A-3 的 SparkApplication。

镜像地址：

`swr.cn-north-4.myhuaweicloud.com/cce-coursefjs/pyspark-douban:v1`
