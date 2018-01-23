package com.upplication.s3fs;

import com.amazonaws.services.s3.model.UploadPartRequest;
import com.github.davidmoten.rx2.Bytes;
import com.sun.tools.javac.util.Pair;
import io.reactivex.Flowable;
import lombok.Builder;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * https://docs.aws.amazon.com/sdkfornet1/latest/apidocs/html/M_Amazon_S3_AmazonS3Client_UploadPart.htm
 */
@Builder
public class UploadStream {

    private final InputStream inputStream;

    @Builder.Default
    private final Integer bufferSize;

    private final UploadTaskExecutor uploadTaskExecutor;

    public void execute() {
        Flowable.zip(
                integerSequence(),
                Bytes.from(inputStream, bufferSize),
                Pair::new
        ).map((pair) -> {
            int partNo = pair.fst;
            byte[] content = pair.snd;

            return new UploadPartRequest()
                    .withUploadId("abc")
                    .withPartNumber(partNo)
                    .withPartSize(content.length)
                    .withInputStream(new ByteArrayInputStream(content));
            // .withBucketName(s3MultipartFileUploader.bucket)
            // .withKey(s3MultipartFileUploader.key)
        }).flatMap(task -> Flowable.fromCallable(() -> uploadTaskExecutor.execute(task)))
                .toList()
                .blockingGet();
    }

    private Flowable<Integer> integerSequence() {
        return Flowable.generate(() -> 0,
                (s, emitter) -> {
                    emitter.onNext(s);
                    return s + 1;
                });
    }

}
