package com.upplication.s3fs;

import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.upplication.s3fs.util.IOUtils;
import lombok.SneakyThrows;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;

public class UploadStreamTest implements UploadTaskExecutor {

    private static final int CHECK_FOR_ABSENCE_TIMEOUT = 1000;
    private static final int CHECK_FOR_PRESENCE_TIMEOUT = 2000;

    private final List<UploadPartRequest> tasksGot = new CopyOnWriteArrayList<>();

    @Test
    public void canBatchBytesSent() {
        UploadStream.builder()
                .inputStream(new ByteArrayInputStream(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }))
                .uploadTaskExecutor(this)
                .bufferSize(5)
                .build().execute();

        assertThat(tasksGot).extracting("partNumber").containsExactly(0, 1, 2);
        assertThat(tasksGot).extracting("partSize").containsExactly(5L, 5L, 2L);
        assertThat(tasksGot).extracting(assertion -> IOUtils.toByteArray(assertion.getInputStream())).containsExactly(
                new byte[] { 1, 2, 3, 4, 5 },
                new byte[] { 6, 7, 8, 9, 10 },
                new byte[] { 11, 12 }
        );
    }

    @Test
    public void doesNotUploadNewPartIfBufferSmaller() throws InterruptedException {
        final BlockingDeque<Byte> unreadBytesStack = new LinkedBlockingDeque<>();

        uploadStreamFromStackAsync(unreadBytesStack);

        unreadBytesStack.offer(new Byte("0"));
        unreadBytesStack.offer(new Byte("1"));

        Thread.sleep(CHECK_FOR_ABSENCE_TIMEOUT);

        assertThat(tasksGot).isEmpty();
    }

    @Test
    public void uploadNewPartOnceBufferAssembled() throws InterruptedException {
        final BlockingDeque<Byte> unreadBytesStack = new LinkedBlockingDeque<>();

        uploadStreamFromStackAsync(unreadBytesStack);

        unreadBytesStack.offer(new Byte("0"));
        unreadBytesStack.offer(new Byte("1"));

        Thread.sleep(CHECK_FOR_ABSENCE_TIMEOUT);

        assertThat(tasksGot).extracting("partNumber").isEmpty();

        unreadBytesStack.offer(new Byte("2"));

        synchronized (tasksGot) {
            tasksGot.wait(CHECK_FOR_PRESENCE_TIMEOUT);
        }

        assertThat(tasksGot).extracting("partNumber").containsExactly(0);
        assertThat(tasksGot).extracting("partSize").containsExactly(3L);
        assertThat(tasksGot).extracting(assertion -> IOUtils.toByteArray(assertion.getInputStream())).containsExactly(
                new byte[] { 0, 1, 2 }
        );
    }

    @Test
    public void sendsTheLastPartEvenIfNotEnoughBytes() throws InterruptedException {
        uploadStreamFromPredefinedValues(5, 6);

        synchronized (tasksGot) {
            tasksGot.wait(CHECK_FOR_PRESENCE_TIMEOUT);
        }

        assertThat(tasksGot).extracting("partNumber").containsExactly(0);
        assertThat(tasksGot).extracting("partSize").containsExactly(2L);
        assertThat(tasksGot).extracting(assertion -> IOUtils.toByteArray(assertion.getInputStream())).containsExactly(
                new byte[] { 5, 6 }
        );
    }

    @Override
    public UploadPartResult execute(UploadPartRequest uploadPartRequest) {
        synchronized (tasksGot) {
            tasksGot.add(uploadPartRequest);
            tasksGot.notifyAll();
        }
        return new UploadPartResult();
    }

    private void uploadStreamFromPredefinedValues(Integer... values) {
        final Deque<Integer> bytesStack = Stream.of(values).collect(toCollection(ArrayDeque::new));

        new Thread(() -> {
            UploadStream.builder()
                    .inputStream(new InputStream() {

                        @Override
                        @SneakyThrows
                        public int read() throws IOException {
                            return Optional.ofNullable(bytesStack.poll()).orElse(-1);
                        }

                    })
                    .bufferSize(3)
                    .uploadTaskExecutor(this)
                    .build().execute();
        }).start();
    }

    private void uploadStreamFromStackAsync(BlockingDeque<Byte> unreadBytesStack) {
        new Thread(() -> {
            UploadStream.builder()
                    .inputStream(new InputStream() {

                        @Override
                        @SneakyThrows
                        public int read() throws IOException {
                            return unreadBytesStack.takeFirst().intValue();
                        }

                    })
                    .bufferSize(3)
                    .uploadTaskExecutor(this)
                    .build().execute();
        }).start();
    }

}
