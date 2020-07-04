export SPARK_MAJOR_VERSION=2
export obj_path="./"
export obj="create_adhoc_vehicle.R"
export master="--master yarn"
export dmode="--deploy-mode client"

export nexec="--num-executors 32"
export ecores="--executor-cores 2"
export emem="--executor-memory 8g"
export emem_oh="--conf spark.executor.memoryOverhead=4g"

export dcores="--driver-cores 2"
export dmem="--driver-memory 2g"
export dmem_oh="--conf spark.driver.memoryOverhead=4g"
export queue="--conf spark.yarn.queue=ad-hoc"
export maxfield="--conf spark.debug.maxToStringFields=100"

export conf_00="--conf spark.hive.exec.dynamic.partition=true  --conf spark.hive.exec.dynamic.partition.mode=nonstrict"
export conf_01="--conf spark.dynamicAllocation.enabled=true"
export conf_02="--conf spark.dynamicAllocation.minExecutors=6  --conf spark.dynamicAllocation.maxExecutors=16  --conf spark.dynamicAllocation.executorIdleTimeout=600s"
export conf_03="--conf spark.shuffle.service.enabled=true"
export conf_04="--conf spark.reducer.maxSizeInFlight=96m"
export conf_05="--conf spark.shuffle.file.buffer=1m"
export conf_06="--conf spark.memory.offHeap.enabled=true  --conf spark.memory.offHeap.size=3g"
export conf_07="--conf spark.sql.shuffle.partitions=600"
export conf_08="--conf spark.sql.autoBroadcstJoinThreshold=21g --conf spark.sql.broadcastTimeout=4800"
export conf_09="--conf spark.orc.create_index=true --conf spark.orc.stripe.size=268435456 --conf spark.orc.row.index.stride=10000"
export conf_10="--conf spark.hive.enforce.sorting=true"
export conf_11="--conf spark.orc.compress=NONE"
export conf_12="--conf spark.hive.optimize.bucketmapjoin=true --conf spark.hive.vectorized.execution.enabled=true"
export conf_13="--conf spark.hive.auto.convert.join=true  --conf spark.hive.enforce.bucketing=true "
export conf_14="--conf spark.hive.exec.parallel=true"
export conf_15="--conf spark.serializer=org.apache.spark.serializer.KryoSerializer"
export conf_16="--conf spark.kryoserializer.buffer.max=128m"

kinit -kt /etc/security/keytabs/X981138.keytab X981138@NMCORP.NISSAN.BIZ
spark-submit ${master} ${dmode} ${nexec} ${ecores} ${emem} ${emem_oh} ${dcores} ${dmem} ${dmem_oh} ${queue} ${maxfield} ${conf_15} ${conf_16} ${conf_03} ${conf_04} ${conf_05} ${conf_06} ${conf_07} ${conf_08} ${conf_09} ${conf_10} ${conf_11} ${conf_12} ${conf_13} ${conf_14} ${obj_path}${obj}
result=$?
exit $result
