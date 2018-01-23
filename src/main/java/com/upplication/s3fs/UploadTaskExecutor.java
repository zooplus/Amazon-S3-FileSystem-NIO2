package com.upplication.s3fs;

import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

public interface UploadTaskExecutor {

    UploadPartResult execute(UploadPartRequest uploadPartRequest);

}
