FROM swr.cn-north-4.myhuaweicloud.com/cce-coursefjs/pyspark:v9

USER root
COPY douban_movies.csv /opt/spark/work/douban_movies.csv
COPY douban_clean.py /opt/spark/work/douban_clean.py
COPY douban_analyze.py /opt/spark/work/douban_analyze.py
COPY douban_benchmark.py /opt/spark/work/douban_benchmark.py
RUN chmod 644 /opt/spark/work/douban_movies.csv && chmod 755 /opt/spark/work/douban_*.py
