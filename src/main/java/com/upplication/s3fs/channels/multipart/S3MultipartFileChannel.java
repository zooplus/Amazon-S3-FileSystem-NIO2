package com.upplication.s3fs.channels.multipart;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.github.davidmoten.guavamini.Sets;
import com.upplication.s3fs.S3Path;
import com.upplication.s3fs.channels.S3Uploader;
import io.reactivex.Single;
import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subjects.Subject;
import lombok.SneakyThrows;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static com.upplication.s3fs.AmazonS3Factory.MULTIPART_PART_SIZE;
import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.READ;

public class S3MultipartFileChannel extends FileChannel {

    private static final long DEFAULT_PART_SIZE = 16 * 1024 * 1024; // 16MB

    private final Set<? extends OpenOption> options;
    private final FileChannel backingFileChannel;
    private final Path backingFilePath;
    private final Subject<PartKey> partKeySubject = ReplaySubject.create();
    private final ObjectMetadata objectMetadata = new ObjectMetadata();
    private final Single<MultipartUploadSummary> multipartUploadSummary;
    private final S3Path path;

    public S3MultipartFileChannel(S3Path path, Set<? extends OpenOption> options, Properties properties) throws IOException {
        this.options = Collections.unmodifiableSet(new HashSet<>(options));
        this.path = path;
        String key = path.getKey();
        boolean exists = path.getFileSystem().provider().exists(path);

        if (exists && this.options.contains(StandardOpenOption.CREATE_NEW))
            throw new FileAlreadyExistsException(format("target already exists: %s", path));
        else if (!exists && !this.options.contains(StandardOpenOption.CREATE_NEW) &&
                !this.options.contains(StandardOpenOption.CREATE))
            throw new NoSuchFileException(format("target not exists: %s", path));

        backingFilePath = Files.createTempFile("temp-s3-", key.replaceAll("/", "_"));
        boolean removeTempFile = true;
        try {
            if (exists) {
                try (S3Object object = path.getFileSystem()
                        .getClient()
                        .getObject(path.getFileStore().getBucket().getName(), key)) {
                    Files.copy(object.getObjectContent(), backingFilePath, StandardCopyOption.REPLACE_EXISTING);
                }
            }

            Set<? extends OpenOption> fileChannelOptions = new HashSet<>(this.options);
            fileChannelOptions.remove(StandardOpenOption.CREATE_NEW);
            backingFileChannel = FileChannel.open(backingFilePath, fileChannelOptions);
            removeTempFile = false;

            multipartUploadSummary = S3MultipartUploader.builder()
                    .path(path)
                    .s3Client(path.getFileSystem().getClient())
                    .objectMetadata(objectMetadata)
                    .changingParts(partKeySubject)
                    .uploadChannel(FileChannel.open(backingFilePath, Sets.newHashSet(READ)))
                    .partSize(Long.parseLong(properties.getProperty(MULTIPART_PART_SIZE, String.valueOf(DEFAULT_PART_SIZE))))
                    .build()
                    .upload(partKeySubject::onComplete);

        } catch (Exception e) {
            partKeySubject.onError(e);
            throw new IllegalStateException(e);
        } finally {
            if (removeTempFile) {
                Files.deleteIfExists(backingFilePath);
            }
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return backingFileChannel.read(dst);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return backingFileChannel.read(dsts, offset, length);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        final long startingPosition = position();
        int bytesWritten = backingFileChannel.write(src);

        partKeySubject.onNext(
                PartKey.builder()
                        .start(startingPosition)
                        .length(bytesWritten)
                        .build()
        );

        return bytesWritten;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        final long startingPosition = position();
        final long bytesWritten = backingFileChannel.write(srcs, offset, length);
        partKeySubject.onNext(
                PartKey.builder()
                        .start(startingPosition)
                        .length(bytesWritten)
                        .build()
        );
        return bytesWritten;
    }

    @Override
    public long position() throws IOException {
        return backingFileChannel.position();
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        backingFileChannel.position(position());
        return this;
    }

    @Override
    public long size() throws IOException {
        return backingFileChannel.size();
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
//        long oldSize = backingFileChannel.size();
        return backingFileChannel.truncate(size);
    }

    @Override
    public void force(boolean metaData) throws IOException {
        backingFileChannel.force(metaData);
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        return backingFileChannel.transferTo(position, count, target);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        long bytesWritten = backingFileChannel.transferFrom(src, position, count);
        partKeySubject.onNext(
                PartKey.builder()
                        .start(position)
                        .length(bytesWritten)
                        .build()
        );
        return bytesWritten;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return backingFileChannel.read(dst, position);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        int bytesWritten = backingFileChannel.write(src, position);
        partKeySubject.onNext(
                PartKey.builder()
                        .start(position)
                        .length(bytesWritten)
                        .build()
        );
        return bytesWritten;
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        return backingFileChannel.map(mode, position, size);
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        return backingFileChannel.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return backingFileChannel.tryLock(position, size, shared);
    }

    @Override
    protected void implCloseChannel() throws IOException {
        if (!this.options.contains(READ)) {
            completeUpload();
        }
        super.close();
        backingFileChannel.close();
        Files.deleteIfExists(backingFilePath);
    }

    @SneakyThrows
    private void completeUpload() {
        MultipartUploadSummary summary = multipartUploadSummary.blockingGet();
        if (!summary.isPerformed()) {
            InputStream in = Channels.newInputStream(FileChannel.open(backingFilePath, Sets.newHashSet(READ)));
            S3Uploader.builder()
                    .path(path)
                    .metadata(objectMetadata)
                    .in(in)
                    .size(summary.getBytesReceived())
                    .build()
                    .upload();
        }
    }

}
