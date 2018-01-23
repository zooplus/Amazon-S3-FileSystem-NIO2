package com.upplication.s3fs;

import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.InputStream;

@Builder
@ToString
class UploadPartTask {

//    private final S3MultipartFileUploader s3MultipartFileUploader;
    private final String uploadId;
    private final int partNumber;
    private final long length;
    private final InputStream stream;

    public UploadPartResult call() throws Exception {






//        UploadPartResult uploadResult = s3MultipartFileUploader.s3Client.uploadPart(request);

//        s3MultipartFileUploader.updateBytesTransferred(length);

        return null; // uploadResult;
    }

}
