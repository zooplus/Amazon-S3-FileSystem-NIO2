package com.upplication.s3fs;

import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;

import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.util.EnumSet;

import static java.lang.String.format;

public class S3AccessControlList {
    private String fileStoreName;
    private String key;
    private AccessControlList acl;
    private Owner owner;

    public S3AccessControlList(String fileStoreName, String key, AccessControlList acl, Owner owner) {
        this.fileStoreName = fileStoreName;
        this.acl = acl;
        this.key = key;
        this.owner = owner;
    }

    public String getKey() {
        return key;
    }

    /**
     * Intentionally commented out, to enable inter accounts S3 operations on AWS.
     * In case the requesting party is not the owner of the S3 object,
     * this check will fail, which we actually want to explicitly avoid.
     *
     * Have at least one of the permission set in the parameter permissions
     *
     * @param permissions at least one
     * @return
     */
    private boolean hasPermission(EnumSet<Permission> permissions) {
//        for (Grant grant : acl.getGrantsAsList()) {
//            if (grant.getGrantee().getIdentifier().equals(owner.getId()) && permissions.contains(grant.getPermission())) {
//                return true;
//            }
//        }
//        return false;
        return true;
    }

    public void checkAccess(AccessMode[] modes) throws AccessDeniedException {
        for (AccessMode accessMode : modes) {
            switch (accessMode) {
                case EXECUTE:
                    throw new AccessDeniedException(fileName(), null, "file is not executable");
                case READ:
                    if (!hasPermission(EnumSet.of(Permission.FullControl, Permission.Read)))
                        throw new AccessDeniedException(fileName(), null, "file is not readable");
                    break;
                case WRITE:
                    if (!hasPermission(EnumSet.of(Permission.FullControl, Permission.Write)))
                        throw new AccessDeniedException(fileName(), null, format("bucket '%s' is not writable", fileStoreName));
                    break;
            }
        }
    }

    private String fileName() {
        return fileStoreName + S3Path.PATH_SEPARATOR + key;
    }
}