package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Assert;
import org.junit.Test;

public class BufferPoolTest_20221201 {

    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(time);
    private final String metricGroup = "TestMetrics";
    private final long maxBlockTimeMs = 2000;

    @Test
    public void testAllocateBufferPoolWhenNonPooledAvailableMemoryIsAvailable() throws Exception {
        int totalMemory = 10 * 1024; //buffer.memory 설정
        int batchSize = 1024; //batch.size 설정
        BufferPool bufferPool = new BufferPool(
            totalMemory,
            batchSize,
            metrics,
            time,
            metricGroup
        );

        int sizeToAllocate = 100;
        ByteBuffer allocated = bufferPool.allocate(sizeToAllocate, maxBlockTimeMs);
        Assert.assertEquals("100 바이트 크기의 버퍼가 할당된다", sizeToAllocate, allocated.limit());
        Assert.assertEquals("bufferPool에 반환되지 않은 버퍼는 free에 추가되지 않는다",0, bufferPool.freeSize());
        Assert.assertEquals("nonPoolableAvailableMemory", totalMemory - sizeToAllocate, bufferPool.unallocatedMemory());

        bufferPool.deallocate(allocated);
        Assert.assertEquals("반환된 버퍼의 크기 == batchSize인 경우에만 free에 추가된다.", 0, bufferPool.freeSize());
        Assert.assertEquals(totalMemory, bufferPool.availableMemory());

        sizeToAllocate = batchSize;
        allocated = bufferPool.allocate(sizeToAllocate, maxBlockTimeMs);
        Assert.assertEquals("batchSize 크기의 버퍼가 할당된다", batchSize, allocated.limit());
        Assert.assertEquals("free에는 아직 아무것도 추가되지 않은 상태이다",0, bufferPool.freeSize());
        bufferPool.deallocate(allocated);
        Assert.assertEquals("batchSize 크기의 버퍼를 bufferPool에 반환하면, 반환된 버퍼는 free에 추가된다",1, bufferPool.freeSize());
        Assert.assertEquals("nonPooledAvailableMemory값은 증가하지 않은 상태이다 (실제로 메모리가 반환되지는 않았으므로)",totalMemory - batchSize, bufferPool.unallocatedMemory());

    }

    @Test
    public void testAllocateBufferPoolWhenNonPooledAvailableMemoryIsNotAvailable() throws Exception {
        int totalMemory = 3 * 1024; //buffer.memory 설정
        int batchSize = 1024; //batch.size 설정
        BufferPool bufferPool = new BufferPool(
            totalMemory,
            batchSize,
            metrics,
            time,
            metricGroup
        );

        bufferPool.allocate(batchSize, maxBlockTimeMs);
        bufferPool.allocate(batchSize, maxBlockTimeMs);
        ByteBuffer bufferToDeallocate = bufferPool.allocate(batchSize, maxBlockTimeMs);
        Assert.assertEquals(0, bufferPool.availableMemory()); //free + nonPooled

        bufferPool.deallocate(bufferToDeallocate); //free에 추가됨
        Assert.assertEquals("반환된 버퍼의 크기 == batchSize이므로 nonPooled에 반환되지 않는다", 0, bufferPool.unallocatedMemory()); //nonPooled = 0
        Assert.assertEquals("반환된 버퍼의 크기 == batchSize이므로 free에 추가된다", 1, bufferPool.freeSize());
        Assert.assertEquals("free + nonPooledAvailableMemory == 반환된 버퍼 크기", bufferToDeallocate.capacity(), bufferPool.availableMemory());
    }

    @Test
    public void testBufferIsAllocatedFromFree() throws Exception {
        int totalMemory = 3 * 1024; //buffer.memory 설정
        int batchSize = 1024; //batch.size 설정
        BufferPool bufferPool = new BufferPool(
            totalMemory,
            batchSize,
            metrics,
            time,
            metricGroup
        );

        ByteBuffer allocated = bufferPool.allocate(batchSize, maxBlockTimeMs);
        bufferPool.deallocate(allocated);
        Assert.assertEquals(1, bufferPool.freeSize());

        bufferPool.allocate(batchSize, maxBlockTimeMs);
        Assert.assertEquals(0, bufferPool.freeSize());
    }

    @Test
    public void testTimeoutExceptionWhenWaitingTimeIsElapsed() throws Exception {
        int totalMemory = 3 * 1024; //buffer.memory 설정
        int batchSize = 1024; //batch.size 설정
        BufferPool bufferPool = new BufferPool(
            totalMemory,
            batchSize,
            metrics,
            time,
            metricGroup
        );
        bufferPool.allocate(batchSize, maxBlockTimeMs);
        bufferPool.allocate(batchSize, maxBlockTimeMs);
        bufferPool.allocate(batchSize, maxBlockTimeMs);

        TimeoutException timeoutException = Assert.assertThrows(TimeoutException.class, () -> {
            bufferPool.allocate(batchSize, maxBlockTimeMs); //blocked
        });
        Assert.assertEquals("Failed to allocate memory within the configured max blocking time 2000 ms.", timeoutException.getMessage());
    }

    @Test
    public void testTimeoutExceptionWhenWaitingTimeIsElapsed2() throws Exception {
        long deallocAfter = 600;
        int totalMemory = 3 * 1024; //buffer.memory 설정
        int batchSize = 1024; //batch.size 설정
        BufferPool bufferPool = new BufferPool(
            totalMemory,
            batchSize,
            metrics,
            time,
            metricGroup
        );
        bufferPool.allocate(batchSize, maxBlockTimeMs);
        bufferPool.allocate(batchSize, maxBlockTimeMs);
        bufferPool.allocate(512, maxBlockTimeMs);
        ByteBuffer allocate = bufferPool.allocate(512, maxBlockTimeMs);
        Assert.assertEquals(0, bufferPool.freeSize());

        asyncDeAllocate(allocate, bufferPool, deallocAfter);
        Assert.assertThrows(TimeoutException.class, () -> {
            bufferPool.allocate(batchSize, maxBlockTimeMs);
        });
        Assert.assertEquals(0, bufferPool.freeSize());
        Assert.assertEquals(512, bufferPool.unallocatedMemory());
    }

    @Test
    public void testBufferAllocateWithInRemainingTime() throws Exception {
        long deallocAfter = 600;
        int totalMemory = 3 * 1024; //buffer.memory 설정
        int batchSize = 1024; //batch.size 설정
        BufferPool bufferPool = new BufferPool(
            totalMemory,
            batchSize,
            metrics,
            time,
            metricGroup
        );
        bufferPool.allocate(batchSize, maxBlockTimeMs);
        bufferPool.allocate(batchSize, maxBlockTimeMs);
        ByteBuffer allocate = bufferPool.allocate(batchSize, maxBlockTimeMs);
        Assert.assertEquals(0, bufferPool.freeSize());

        asyncDeAllocate(allocate, bufferPool, deallocAfter);
        bufferPool.allocate(batchSize, maxBlockTimeMs);
        Assert.assertEquals(0, bufferPool.freeSize());
        Assert.assertEquals("버퍼 할당 대기중인 요청이 없으면 Condition이 삭제된다", 0, bufferPool.queued());
    }

    @Test
    public void test2ConditionsAreCreated() throws Exception {
        int totalMemory = 3 * 1024; //buffer.memory 설정
        int batchSize = 1024; //batch.size 설정
        BufferPool bufferPool = new BufferPool(
            totalMemory,
            batchSize,
            metrics,
            time,
            metricGroup
        );
        bufferPool.allocate(batchSize, maxBlockTimeMs);
        bufferPool.allocate(batchSize, maxBlockTimeMs);
        bufferPool.allocate(batchSize, maxBlockTimeMs);


        new Thread(() -> {
            try {
                Thread.sleep(1000);
                Assert.assertEquals(2, bufferPool.queued());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        Assert.assertThrows(TimeoutException.class, () -> {
            bufferPool.allocate(batchSize, maxBlockTimeMs);
        });
    }

    private Thread asyncDeAllocate(ByteBuffer buffer, BufferPool bufferPool, Long time) {
        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(time);
                bufferPool.deallocate(buffer);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        thread.start();
        return thread;
    }


}
