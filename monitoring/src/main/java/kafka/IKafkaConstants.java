package kafka;

public interface IKafkaConstants {
    public static String KAFKA_BROKERS = "10.0.0.5:9092";

    public static Integer MESSAGE_COUNT = 1000;

    public static String CLIENT_ID = "dp-test-client";

    public static String GROUP_ID_CONFIG="dp-test-group";

    public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;

    public static String OFFSET_RESET_LATEST = "latest";

    public static String OFFSET_RESET_EARLIEST="earliest";

    public static Integer MAX_POLL_RECORDS = 1;
}