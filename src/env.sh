export env=dev

export kafka_ip=${KAFKA_HOST}
export kafka_port=9092

export kafka_topic_ingest=telemetry.ingest
export kafka_topic_raw=telemetry.raw
export kafka_topic_unique=telemetry.unique
export kafka_topic_de_norm=telemetry.denorm
export kafka_topic_druid_events=druid.events.telemetry

export kafka_topic_logs=druid.events.log

export kafka_topic_failed=telemetry.failed
export kafka_topic_batch_failed=telemetry.extractor.failed
export kafka_topic_duplicate=telemetry.duplicate
export kafka_topic_batch_duplicate=telemetry.extractor.duplicate