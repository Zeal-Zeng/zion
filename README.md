# zion
a data warehouse project base on spark&amp;hive  基于spark 和hive的离线数据仓库

# quick start
## 1、在pyspark ipython 中
```
import zion
zion.set_env("dev")
mysql_tb = zion.MysqlExtractor("test_db","test_tb")
df = mysql_tb.gen_df()
df.show()
```

## 2、spark-submit中
```
spark-submit zion/start.py module="zion.ods.mysql2hive" db=test_db tb=test_tb 
```