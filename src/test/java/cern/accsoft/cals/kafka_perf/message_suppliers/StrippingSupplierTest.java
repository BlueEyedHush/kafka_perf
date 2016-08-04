package cern.accsoft.cals.kafka_perf.message_suppliers;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class StrippingSupplierTest {
    @Test
    public void correctFirstAndIncrementShouldBeReturned0() {
        /* 4 producers, each has 4 threads
         * we simulate 1st producer's (id 0) 4th thread (id 3), so messages
         * so first == 3, increment == 16 */
        int[] results = StrippingSupplier.getFirstAndIncrement(0, 4, 3, 4);
        Assert.assertArrayEquals(new int[]{12,16}, results);
    }

    @Test
    public void correctFirstAndIncrementShouldBeReturned1() {
        /* 3 producers, each has 8 threads
         * we simulate 2nd producer's (id 1) 5th thread (id 4), so messages
         * so first == 18, increment == 32 */
        int[] results = StrippingSupplier.getFirstAndIncrement(1, 3, 4, 8);
        Assert.assertArrayEquals(new int[]{13,24}, results);
    }

    @Test
    public void messagesShouldHaveCorrectTopics() {
        /* 4 producers, each has 4 threads, we have 32 topics
         * we simulate 1st producer's (id 0) 4th thread (id 3), so messages
         * so only messages with topic 3 and 19 should be returned */
        int[] results = StrippingSupplier.getFirstAndIncrement(0, 4, 3, 4);
        MessageSupplier supplier = new StrippingSupplier(1, results[0], results[1], 32, 4);

        int[] topics = new int[6];
        for(int i = 0; i < 6; i++) {
            topics[i] = Integer.valueOf(supplier.get().topic(), 10);
        }

        Assert.assertArrayEquals(new int[]{12,28,12,28,12,28}, topics);
    }

    @Test
    public void topicsShouldBeEquallyUtilized() {
        int instances = 8;
        int threads = 4;
        int topics = 4096;
        int iteratations = 128;

        List<MessageSupplier> supplierList = new ArrayList<>(instances*threads);
        int[] messagesInTopic = new int[topics];

        for(int i = 0; i < instances; i++) {
            for(int j = 0; j < threads; j++) {
                int[] results = StrippingSupplier.getFirstAndIncrement(i, instances, j, threads);
                supplierList.add(new StrippingSupplier(1, results[0], results[1], topics, 4));
            }
        }

        for(int i = 0; i < iteratations; i++) {
            supplierList.forEach(s -> {
                messagesInTopic[Integer.valueOf(s.get().topic(), 10)]++;
            });
        }

        int expectedPerTopic = topics/(instances*threads*iteratations);
        for(int i = 0; i < topics; i++) {
            Assert.assertEquals(expectedPerTopic, messagesInTopic[i]);
        }
    }

    @Test
    public void partitionsShouldBeEquallyUtilized() {
        int instances = 8;
        int threads = 4;
        int partitions = 4096;
        int iteratations = 128;

        List<MessageSupplier> supplierList = new ArrayList<>(instances*threads);
        int[] messagesInPartition = new int[partitions];

        for(int i = 0; i < instances; i++) {
            for(int j = 0; j < threads; j++) {
                int[] results = StrippingSupplier.getFirstAndIncrement(i, instances, j, threads);
                supplierList.add(new StrippingSupplier(1, results[0], results[1], 4, partitions));
            }
        }

        for(int i = 0; i < iteratations; i++) {
            supplierList.forEach(s -> {
                messagesInPartition[s.get().partition()]++;
            });
        }

        int expectedPerPartition = partitions/(instances*threads*iteratations);
        for(int i = 0; i < partitions; i++) {
            Assert.assertEquals(expectedPerPartition, messagesInPartition[i]);
        }
    }
}