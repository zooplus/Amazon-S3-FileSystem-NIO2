package com.upplication.s3fs;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.github.davidmoten.rx2.Bytes;
import com.sun.tools.javac.util.Pair;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.functions.BiFunction;
import io.reactivex.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.amazonaws.event.ProgressInputStream.inputStreamForRequest;

public class S3MultipartFileUploader implements Runnable {

    @Override
    public void run() {

    }

//    private static final Logger log = LoggerFactory.getLogger(S3MultipartFileUploader.class);
//
//    public static final int DEFAULT_THREADS = 8;
//
//    public static final long MIN_PART_SIZE = 4 * 1024 * 1024; // 4MB
//    public static final long DEFAULT_PART_SIZE = 128 * 1024 * 1024; // 128MB
//    public static final int MAX_PARTS = 10000;
//
//    private AmazonS3Client s3Client;
//    private String bucket;
//    private String key;
//    private ObjectMetadata objectMetadata;
//    private InputStream stream;
//    private boolean closeStream = true;
//    private long fullSize;
//    private Long partSize = DEFAULT_PART_SIZE;
//    private int threads = DEFAULT_THREADS;
//    private ExecutorService executorService;
//    private AtomicLong bytesTransferred = new AtomicLong();
//    private ProgressListener progressListener;
//
//    public S3MultipartFileUploader(
//            AmazonS3Client s3Client,
//            String bucket,
//            String key,
//            InputStream stream,
//            long size
//    ) {
//        this.s3Client = s3Client;
//        this.bucket = bucket;
//        this.key = key;
//        this.stream = stream;
//        this.fullSize = size;
//    }
//
//    @Override
//    public void run() {
//        doMultipartUpload();
//    }
//
//    public void doMultipartUpload() {
//        configure();
//
//        // initiate MP upload
//        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket, key);
//        initRequest.setObjectMetadata(objectMetadata);
//        String uploadId = s3Client.initiateMultipartUpload(initRequest).getUploadId();
//
//        Stack<Future<UploadPartResult>> futureStack = new Stack<>();
//        try {
//
//            Flowable<Integer> sequenceStream = Flowable.just(1, 2, 3);
//
//            final Flowable<byte[]> flowable = Bytes.from(stream);
//
//            int partNumber = 1;
//            long offset = 0, length = partSize;
//
//            flowable.delaySubscription(1, TimeUnit.SECONDS)
//                    .blockingForEach();
//
//
//            Flowable.zip(sequenceStream, flowable, (BiFunction<Integer, byte[], Pair>) Pair::new)
//                    .forEach((pair) -> {
//                        new UploadPartTask(uploadId, pair.fst, offset, length).call();
//                    });
//
//
//
//
//            // submit all upload tasks
//
//
//            while (offset < fullSize) {
//                if (offset + length > fullSize) length = fullSize - offset;
//
//                futureStack.add(executorService.submit();
//
//                offset += length;
//            }
//
//            SortedSet<UploadPartResult> parts = new TreeSet<>();
//            for (Future<UploadPartResult> future : futureStack) {
//                parts.add(future.get());
//            }
//
//            // complete MP upload
//            CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
//                    bucket,
//                    key,
//                    uploadId,
//                    parts.stream().map(UploadPartResult::getPartETag)
//                            .collect(Collectors.toList())
//            );
//
//            CompleteMultipartUploadResult result = s3Client.completeMultipartUpload(compRequest);
//            eTag = result.getETag();
//
//        } catch (Exception e) {
//            try {
//                s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadId));
//            } catch (Throwable t) {
//                log.warn("could not abort upload after failure", t);
//            }
//            throw new RuntimeException("error during upload", e);
//        } finally {
//            executorService.shutdown();
//            if (stream != null && closeStream) {
//                try {
//                    stream.close();
//                } catch (Throwable t) {
//                    log.warn("could not close stream", t);
//                }
//            }
//        }
//    }
//
//    private void configure() {
//
//        if (file != null) {
//            if (!file.exists() || !file.canRead())
//                throw new IllegalArgumentException("cannot read file: " + file.getPath());
//
//            fullSize = file.length();
//        } else {
//            if (stream == null)
//                throw new IllegalArgumentException("must specify a file or stream to read");
//
//            // make sure size is set
//            if (fullSize <= 0)
//                throw new IllegalArgumentException("size must be specified for stream");
//
//            // must read stream sequentially
//            executorService = null;
//            threads = 1;
//        }
//
//        // make sure content-length isn't set
//        if (objectMetadata != null) {
//            // objectMetadata.setContentLength(null);
//        }
//
//        long minPartSize = Math.max(MIN_PART_SIZE, fullSize / MAX_PARTS + 1);
//        log.debug(String.format("minimum part size calculated as %,dk", minPartSize / 1024));
//
//        if (partSize == null) partSize = minPartSize;
//        if (partSize < minPartSize) {
//            log.warn(String.format("%,dk is below the minimum part size (%,dk). the minimum will be used instead",
//                    partSize / 1024, minPartSize / 1024));
//            partSize = minPartSize;
//        }
//
//        if (executorService == null) {
//            executorService = Executors.newFixedThreadPool(threads);
//        }
//    }
//
//    private void updateBytesTransferred(long count) {
//        long totalTransferred = bytesTransferred.addAndGet(count);
//
//        if (progressListener != null) {
//            progressListener.progressChanged(new ProgressEvent(
//                    ProgressEventType.REQUEST_BYTE_TRANSFER_EVENT,
//                    totalTransferred
//            ));
//        }
//    }
//
//    private class UploadPartTask implements Callable<UploadPartResult> {
//        private String uploadId;
//        private int partNumber;
//        private long offset;
//        private long length;
//
//        UploadPartTask(String uploadId, int partNumber, long offset, long length) {
//            this.uploadId = uploadId;
//            this.partNumber = partNumber;
//            this.offset = offset;
//            this.length = length;
//        }
//
//        @Override
//        public UploadPartResult call() throws Exception {
//            UploadPartRequest request = new UploadPartRequest()
//                    .withBucketName(bucket)
//                    .withKey(key)
//                    .withUploadId(uploadId)
//                    .withPartNumber(partNumber)
//                    .withFileOffset(offset)
//                    .withPartSize(length)
//                    .withInputStream(inputStreamForRequest(stream, progressListener));
//
//            UploadPartResult uploadResult = s3Client.uploadPart(request);
//
//            updateBytesTransferred(length);
//
//            return uploadResult;
//        }
//
//    }

}
