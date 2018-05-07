package com.upplication.s3fs.channels.multipart;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.upplication.s3fs.S3Path;
import io.reactivex.Observable;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.tika.Tika;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.stream.Collectors;

import static java.nio.channels.Channels.newInputStream;
import static org.apache.commons.io.IOUtils.read;

@Slf4j
public class S3MultipartUploader extends MultipartUploader<UploadPartResult> {

    private final ObjectMetadata objectMetadata;
    private final AmazonS3 s3Client;
    private final S3Path path;
    private final FileChannel uploadChannel;

    private String uploadId;

    @Builder
    public S3MultipartUploader(
            Observable<PartKey> changingParts,
            ObjectMetadata objectMetadata,
            AmazonS3 s3Client,
            S3Path path,
            FileChannel uploadChannel,
            Long partSize) {
        super(changingParts, partSize);
        this.objectMetadata = objectMetadata;
        this.s3Client = s3Client;
        this.path = path;
        this.uploadChannel = uploadChannel;
    }

    @Override
    protected void startTransfer(List<PartKey> object) {
        final String bucket = path.getFileStore().name();
        final String key = path.getKey();
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket, key);
        try {
            objectMetadata.setContentType(new Tika().detect(newInputStream(uploadChannel), path.getFileName().toString()));
        } catch (IOException e) {
            throw new IllegalStateException("Could not determine content type", e);
        }
        initRequest.setObjectMetadata(objectMetadata);
        uploadId = s3Client.initiateMultipartUpload(initRequest).getUploadId();
    }

    @Override
    protected Part<UploadPartResult> uploadNewPart(int partNo, PartKey partKey) {
        UploadPartResult uploadPartResult = s3Client.uploadPart(uploadPartRequest(partNo, partKey));
        log.info("Uploading file: {}, Part No: {}, Part Length: {}",
                path.toString(),
                partNo,
                partKey.getLength());
        return new Part<>(partKey, partNo, uploadPartResult);
    }

    @Override
    protected String endTransfer() {
        final List<PartETag> partEtags = getManagedParts().values().stream()
                .map(Part::getValue)
                .map(UploadPartResult::getPartETag)
                .collect(Collectors.toList());

        final String bucket = path.getFileStore().name();
        final String key = path.getKey();
        return s3Client.completeMultipartUpload(
                new CompleteMultipartUploadRequest(
                        bucket,
                        key,
                        uploadId,
                        partEtags)).getETag();
    }

    private UploadPartRequest uploadPartRequest(int partId, PartKey partKey) {
        final String bucket = path.getFileStore().name();
        final String key = path.getKey();

        return new UploadPartRequest()
                    .withUploadId(uploadId)
                    .withPartNumber(partId)
                    .withPartSize(partKey.getLength())
                    .withInputStream(asInputStream(uploadChannel, partKey))
                    .withBucketName(bucket)
                    .withKey(key);
    }

    private InputStream asInputStream(FileChannel fileChannel, PartKey partKey) {
        int partOffset = partKey.startAsInt();
        int partLength = partKey.lengthAsInt();

        try {
            // Not being closed intentionally, as it will be closed with the channel
            InputStream channelStream = newInputStream(fileChannel.position(partKey.getStart()));
            byte[] buffer = new byte[partOffset + partLength];
            read(channelStream, buffer, partOffset, partLength);

            return new ByteArrayInputStream(buffer, partOffset, partLength);
        } catch (IOException e) {
            log.error("Failed to upload file part", e);
            throw new RuntimeException(e);
        }
    }

}
