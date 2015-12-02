#!/bin/bash
DATASET=$1

spark/bin/spark-submit \
    --class RunLDA \
    --master spark://$MASTER_DNS:7077 \
    /root/svi-lda/svi-lda-assembly-1.0.jar \
    --dataset $DATASET \
    --data-path hdfs://$MASTER_DNS:9000/svi-lda/data \
    --log-path svi-lda/log \
    --master spark://$MASTER_DNS:7077 \
    --num-partitions 64