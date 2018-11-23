package com.upplication.s3fs.util;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.google.common.collect.Sets;
import com.upplication.s3fs.S3ObjectSummaryCache;
import com.upplication.s3fs.S3Path;
import com.upplication.s3fs.attribute.S3BasicFileAttributes;
import com.upplication.s3fs.attribute.S3PosixFileAttributes;
import com.upplication.s3fs.attribute.S3UserPrincipal;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;

import java.io.FileNotFoundException;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.util.CollectionUtils.isNullOrEmpty;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * Utilities to work with Amazon S3 Objects.
 */
@Slf4j
public class S3Utils {

    /**
     * Get the {@link S3ObjectSummary} that represent this Path or her first child if this path not exists
     *
     * @param s3Path {@link S3Path}
     * @return {@link S3ObjectSummary}
     */
    public S3ObjectSummary getS3ObjectSummary(S3Path s3Path) throws NoSuchFileException {
        String key = s3Path.getKey();
        String bucketName = s3Path.getFileStore().name();
        try {
            return S3ObjectSummaryCache.INSTANCE.getOrCacheDirectory(key)
                    .orElseGet(() -> getFileSummary(s3Path, bucketName, key)

                            // if not found (404 err) with the original key, try to find the element as a directory.
                            .orElseGet(() -> getFolderSummaryFromFirstChildFile(s3Path, bucketName, key)
                                    .orElseThrow(() -> new NoSuchElementException(bucketName + S3Path.PATH_SEPARATOR + key))));
        } catch (NoSuchElementException nse) {
            throw new NoSuchFileException(nse.getMessage());
        }
    }

    private Optional<S3ObjectSummary> getFileSummary(S3Path s3Path, String bucketName, String filePath) {
        try {
            AmazonS3 client = s3Path.getFileSystem().getClient();
            ObjectMetadata metadata = client.getObjectMetadata(bucketName, filePath);

            S3ObjectSummary summary = new S3ObjectSummary();
            summary.setBucketName(bucketName);
            summary.setETag(metadata.getETag());
            summary.setKey(filePath);
            summary.setLastModified(metadata.getLastModified());
            summary.setSize(metadata.getContentLength());
            summary.setOwner(client.getObjectAcl(bucketName, filePath).getOwner());

            return Optional.of(S3ObjectSummaryCache.INSTANCE.put(filePath, summary));
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
                throw e;
            }
        }
        return Optional.empty();
    }

    private Optional<S3ObjectSummary> getFolderSummaryFromFirstChildFile(S3Path s3Path, String bucketName, String folderPath) {
        try {
            AmazonS3 client = s3Path.getFileSystem().getClient();
            // is a virtual directory
            String folderKey = folderPath.endsWith("/") ? folderPath : folderPath.concat("/");

            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(bucketName);
            request.setPrefix(folderKey);
            request.setMaxKeys(1);

            ObjectListing current = client.listObjects(request);

            return isNullOrEmpty(current.getObjectSummaries())
                    ? Optional.empty()
                    : Optional.of(S3ObjectSummaryCache.INSTANCE.put(folderPath, current.getObjectSummaries().get(0)));
        } catch (Exception e) {
            log.warn("Error occurred while getting the folder summary", e);
            return Optional.empty();
        }
    }

    /**
     * getS3FileAttributes for the s3Path
     *
     * @param s3Path S3Path mandatory not null
     * @return S3FileAttributes never null
     */
    public S3BasicFileAttributes getS3FileAttributes(S3Path s3Path) throws NoSuchFileException {
        S3ObjectSummary objectSummary = isNull(s3Path.getObjectSummary()) ? getS3ObjectSummary(s3Path) : s3Path.getObjectSummary();
        return toS3FileAttributes(objectSummary, s3Path.getKey());
    }

    /**
     * get the S3PosixFileAttributes for a S3Path
     *
     * @param s3Path Path mandatory not null
     * @return S3PosixFileAttributes never null
     */
    public S3PosixFileAttributes getS3PosixFileAttributes(S3Path s3Path) throws NoSuchFileException {
        S3ObjectSummary objectSummary = getS3ObjectSummary(s3Path);

        String key = s3Path.getKey();
        String bucketName = s3Path.getFileStore().name();

        S3BasicFileAttributes attrs = toS3FileAttributes(objectSummary, key);
        S3UserPrincipal userPrincipal = null;
        Set<PosixFilePermission> permissions = null;

        if (!attrs.isDirectory()) {
            AmazonS3 client = s3Path.getFileSystem().getClient();
//            AccessControlList acl = client.getObjectAcl(bucketName, key);
            Owner owner = new Owner("asdf", "asdf");//acl.getOwner();

            userPrincipal = new S3UserPrincipal(owner.getId() + ":" + owner.getDisplayName());
            permissions = new HashSet<>(Arrays.asList(PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ));
        } else {
            permissions = toPosixFilePermission(Permission.FullControl);
        }

        return new S3PosixFileAttributes((String) attrs.fileKey(), attrs.lastModifiedTime(),
                attrs.size(), attrs.isDirectory(), attrs.isRegularFile(), userPrincipal, null, permissions);
    }


    /**
     * transform com.amazonaws.services.s3.model.Grant to java.nio.file.attribute.PosixFilePermission
     *
     * @param grants Set grants mandatory, must be not null
     * @return Set PosixFilePermission never null
     * @see #toPosixFilePermission(Permission)
     */
    public Set<PosixFilePermission> toPosixFilePermissions(List<Grant> grants) {
        Set<PosixFilePermission> filePermissions = new HashSet<>();
        for (Grant grant : grants) {
            filePermissions.addAll(toPosixFilePermission(grant.getPermission()));
        }

        return filePermissions;
    }

    /**
     * transform a com.amazonaws.services.s3.model.Permission to a java.nio.file.attribute.PosixFilePermission
     * We use the follow rules:
     * - transform only to the Owner permission, S3 doesnt have concepts like owner, group or other so we map only to owner.
     * - ACP is a special permission: WriteAcp are mapped to Owner upload permission and ReadAcp are mapped to owner read
     *
     * @param permission Permission to map, mandatory must be not null
     * @return Set PosixFilePermission never null
     */
    public Set<PosixFilePermission> toPosixFilePermission(Permission permission) {
        switch (permission) {
            case FullControl:
                return Sets.newHashSet(PosixFilePermission.OWNER_EXECUTE,
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE);
            case Write:
                return Sets.newHashSet(PosixFilePermission.OWNER_WRITE);
            case Read:
                return Sets.newHashSet(PosixFilePermission.OWNER_READ);
            case ReadAcp:
                return Sets.newHashSet(PosixFilePermission.OWNER_READ);
            case WriteAcp:
                return Sets.newHashSet(PosixFilePermission.OWNER_EXECUTE);
        }
        throw new IllegalStateException("Unknown Permission: " + permission);
    }

    /**
     * transform S3ObjectSummary to S3FileAttributes
     *
     * @param objectSummary S3ObjectSummary mandatory not null, the real objectSummary with
     *                      exactly the same key than the key param or the immediate descendant
     *                      if it is a virtual directory
     * @param key           String the real key that can be exactly equal than the objectSummary or
     * @return S3FileAttributes
     */
    public S3BasicFileAttributes toS3FileAttributes(S3ObjectSummary objectSummary, String key) {
        // parse the data to BasicFileAttributes.
        FileTime lastModifiedTime = null;
        if (objectSummary.getLastModified() != null) {
            lastModifiedTime = FileTime.from(objectSummary.getLastModified().getTime(), TimeUnit.MILLISECONDS);
        }
        long size = objectSummary.getSize();
        boolean directory = false;
        boolean regularFile = false;
        String resolvedKey = objectSummary.getKey();
        // check if is a directory and exists the key of this directory at amazon s3
        if (key.endsWith("/") && resolvedKey.equals(key) ||
                resolvedKey.equals(key + "/")) {
            directory = true;
        } else if (key.isEmpty()) { // is a bucket (no key)
            directory = true;
            resolvedKey = "/";
        } else if (!resolvedKey.equals(key) && resolvedKey.startsWith(key)) { // is a directory but not exists at amazon s3
            directory = true;
            // no metadata, we fake one
            size = 0;
            // delete extra part
            resolvedKey = key + "/";
        } else {
            regularFile = true;
        }
        return new S3BasicFileAttributes(resolvedKey, lastModifiedTime, size, directory, regularFile);
    }
}