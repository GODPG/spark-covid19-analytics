echo "======================================================"
echo "开始执行COVID-19 大数据分析"
echo "======================================================"

echo "[1/6] 正在执行基础清洗与统计分析..."
spark-submit covid_analysis.py

echo "[2/6] 正在执行滚动平均与极值分析..."
spark-submit advanced_analysis.py

echo "[3/6] 正在执行重心转移与周末效应..."
spark-submit final_mining.py

echo "[4/6] 正在执行高阶比率与帕累托分析..."
spark-submit high_level_analysis.py

echo "[5/6] 正在执行时滞相关与异常监测..."
spark-submit advanced_mining_v3.py

echo "[6/6] 正在执行 K-Means 聚类与数据科学模块..."
spark-submit ml_and_advanced_analysis.py

echo "======================================================"
echo "所有 Spark 分析任务已完成"
echo "======================================================"

"run_all_tasks.sh" 25L, 895C                                  1,1          顶端
