package com.upplication.s3fs.channels.multipart;

import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subjects.Subject;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.stream.Collectors;

import static com.upplication.s3fs.channels.multipart.MultipartUploader.UploadingState.*;

/**
 * This class monitors changes which happen to the channel and sends (or re-sends) proper multipart requests.
 * <p>
 * {@see https://docs.aws.amazon.com/sdkfornet1/latest/apidocs/html/M_Amazon_S3_AmazonS3Client_UploadPart.htm}
 */
@RequiredArgsConstructor
public abstract class MultipartUploader<T> {

    public static final long DEFAULT_PART_SIZE = 16 * 1024 * 1024; // 16MB

    private final Observable<PartKey> changingParts;
    private final SortedMap<PartKey, Part<T>> managedParts = new TreeMap<>();
    private final BehaviorSubject<UploadingState> uploadState = BehaviorSubject.createDefault(WAITING_FOR_MORE_PARTS);
    private final Subject<Long> newBytes = ReplaySubject.create();
    private final Long partSizeInBytes = DEFAULT_PART_SIZE;

    public final Single<String> upload(Runnable completeHandler) {
        monitorBytesIncoming();
        handleStateChanges();
        handleTransferStart();
        handleNewParts();
//        handleUpdatedParts(); todo this might be necessary
        return Single.defer(() -> {
            completeHandler.run();
            return Single.just(endTransfer());
        });
    }

    private void monitorBytesIncoming() {
        changingParts.map(PartKey::getLength)
                .scan(Long::sum)
                .map(bytes -> Double.valueOf(Math.floor(bytes / partSizeInBytes)).longValue())
                .distinct() // only next multiplies of the part size
                .skip(1) // skip the first one as this is effectively 0
                .subscribe(newBytes::onNext);
    }

    private void handleStateChanges() {
        changingParts
                .buffer(newBytes)
                .firstElement()
                .subscribe((e) -> uploadState.onNext(READY_TO_START));
    }

    private void handleTransferStart() {
        Observable.zip(
                uploadState.filter(UploadingState::canStartTransfer),
                changingParts.buffer(newBytes),
                (a, b) -> b
        )
                .subscribe(initialParts -> {
                    try {
                        startTransfer(initialParts);
                    } catch (Exception e) {
                        uploadState.onError(e);
                        return;
                    }
                    uploadState.onNext(UPLOADING_PARTS);
                });
    }

    private void handleNewParts() {
        Observable.zip(
                uploadState.filter(UploadingState::canUploadNewParts),
                changingParts.buffer(newBytes).flatMap(this::mergeParts),
                (a, b) -> b
        )
                .doOnEach(part -> uploadState.onNext(UPLOADING_PARTS))
                .filter(part -> managedParts.isEmpty() || part.isAfter(managedParts.lastKey()))
                .map(part -> uploadNewPart(managedParts.size() + 1, part))
                .subscribeOn(Schedulers.io())
                .forEach(part -> managedParts.put(part.getKey(), part));
    }

    private void handleUpdatedParts() {
        Observable.zip(
                uploadState.filter(UploadingState::canUploadUpdatedParts),
                changingParts,
                (a, b) -> b
        )
                .doOnEach(part -> uploadState.onNext(UPLOADING_PARTS))
                .flatMap(this::gatherPartsToBeUpdated)
                .subscribeOn(Schedulers.io())
                .subscribe(partKey -> {
                    Part<T> part = reuploadPart(partKey);
                    managedParts.replace(partKey, part);
                });
    }

    private Observable<PartKey> gatherPartsToBeUpdated(PartKey updatedPart) {
        return Observable.fromIterable(managedParts.keySet()
                .stream()
                .filter(managedPart -> managedPart != updatedPart) // this is intentional, if it is the same part then do not attempt to upload
                .filter(managedPart -> managedPart.overlapsWith(updatedPart))
                .collect(Collectors.toList()));
    }

    protected Map<PartKey, Part<T>> getManagedParts() {
        return Collections.unmodifiableMap(managedParts);
    }

    private Observable<PartKey> mergeParts(List<PartKey> partKeys) {
        return Observable.just(partKeys.stream().reduce(partKeys.iterator().next(), PartKey::unionWith));
    }

    protected abstract void startTransfer(List<PartKey> object);

    protected abstract Part<T> uploadNewPart(int partId, PartKey partKey);

    protected abstract Part<T> reuploadPart(PartKey partKey);

    protected abstract String endTransfer();

    public enum UploadingState {
        WAITING_FOR_MORE_PARTS,
        READY_TO_START,
        UPLOADING_PARTS;

        public static boolean canStartTransfer(UploadingState uploadingState) {
            return READY_TO_START == uploadingState;
        }

        public static boolean canUploadNewParts(UploadingState uploadingState) {
            return UPLOADING_PARTS == uploadingState;
        }

        public static boolean canUploadUpdatedParts(UploadingState uploadingState) {
            return UPLOADING_PARTS == uploadingState;
        }

        public static boolean isDone(UploadingState uploadingState) {
            return false;
        }

        public static boolean canEndTransfer(UploadingState uploadingState) {
            return false;
        }
    }

}
