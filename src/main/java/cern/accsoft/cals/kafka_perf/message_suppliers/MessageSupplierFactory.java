package cern.accsoft.cals.kafka_perf.message_suppliers;

/**
 * Created by knawara on 7/21/16.
 */
public class MessageSupplierFactory {
    public static MessageSupplier get(String id, int messageLength, int topicNumber, int partitionNumber) {
        MessageSupplier supplier;
        switch (id.toLowerCase()) {
            case "mtfl":
                supplier = new MultipleTopicFixedLenghtSupplier(messageLength, topicNumber, partitionNumber);
                break;
            case "stfl":
                supplier = new SingleTopicFixedLengthSupplier(messageLength, topicNumber, partitionNumber);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized id: ".concat(id));
        }

        return supplier;
    }
}
