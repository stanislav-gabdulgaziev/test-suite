# Запуск генерации данных в CSV


## Вариант 1. Драйвер на консоли.
```bash
spark3-submit \
	--master yarn \
	--num-executors 3 \
	--archives tpcds-kit.tar.gz#tpcds tpcds_generate_distributed.py \
	--dsdgen-path ./tpcds/tpcds-bin/dsdgen \
	--dist-path ./tpcds/tpcds-bin/tpcds.idx \
	--scale 1 \
	--parallel 20 \
	--output-path hdfs:///tmp/tpc_csv \
	--format csv


nohup spark3-submit \
	--master yarn \
	--num-executors 3 \
	--archives tpcds-kit.tar.gz#tpcds tpcds_generate_distributed.py \
	--dsdgen-path ./tpcds/tpcds-bin/dsdgen \
	--dist-path ./tpcds/tpcds-bin/tpcds.idx \
	--scale 1 \
	--parallel 20 \
	--output-path hdfs:///tmp/tpc_csv \
	--format csv \
	&> tpcds_generate_distributed_$(date +"%Y-%m-%d_%H-%M-%S").log &
```

