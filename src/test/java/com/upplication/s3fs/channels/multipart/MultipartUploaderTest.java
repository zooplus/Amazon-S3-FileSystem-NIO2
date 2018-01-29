package com.upplication.s3fs.channels.multipart;

import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subjects.Subject;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class MultipartUploaderTest {

    private final Subject<PartKey> incomingParts = ReplaySubject.create();

    private final TestMultipartUploader uploader = new TestMultipartUploader(incomingParts);

    @Before
    public void before() {
        startUpload();
    }

    @Test
    public void doesNotUploadWhenLessThanPartSize() {
        incomingParts.onNext(new PartKey(0, inBytes(6)));
        incomingParts.onNext(new PartKey(0, inBytes(6)));
        assertThat(uploader.isWasTransferStarted()).isFalse();
    }

    @Test
    public void startsTransferAfterReceivingMoreThanOnePartOfData() {
        incomingParts.onNext(new PartKey(0, inBytes(9)));
        incomingParts.onNext(new PartKey(0, inBytes(9)));
        assertThat(uploader.isWasTransferStarted()).isTrue();
    }

    private void startUpload() {
        uploader.upload(incomingParts::onComplete);
    }

    private long inBytes(int mb) {
        return mb * 1024 * 1024;
    }

}