package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/buildkite/agent/v4/internal/cache/internal/trace"
	"go.opentelemetry.io/otel/attribute"
)

// s3MPUAPI is the subset of the S3 client used to discover and resume or
// abort incomplete multipart uploads a crashed or interrupted save left behind.
type s3MPUAPI interface {
	ListMultipartUploads(context.Context, *s3.ListMultipartUploadsInput, ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error)
	ListParts(context.Context, *s3.ListPartsInput, ...func(*s3.Options)) (*s3.ListPartsOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// MultipartAborter is an optional Blob capability: abort an in-progress
// multipart upload using whatever context the caller supplies, so a save whose
// own context is already cancelled can still attempt a best-effort abort.
type MultipartAborter interface {
	AbortUpload(ctx context.Context, key, uploadID string) error
}

// errNoResumableUpload reports that no in-progress multipart upload exists
// for the key; the caller should perform a fresh upload. It is not logged as a
// failure — finding nothing is the normal case.
var errNoResumableUpload = errors.New("no resumable multipart upload")

// errStaleUploadAbandoned reports that an incomplete multipart upload was
// found but could not be resumed (it had no parts or an inconsistent layout),
// so it was aborted and the save uploads fresh. Unlike
// errNoResumableUpload it means an upload was found and deliberately
// abandoned, which is reported to the user at Info level.
var errStaleUploadAbandoned = errors.New("incomplete multipart upload abandoned")

// tryResumeUpload looks for an in-progress multipart upload for fullKey and,
// if one exists, uploads only the missing parts and completes it.
//
// Returns errNoResumableUpload when nothing resumable exists. On a validation
// problem (the orphaned upload can never assemble into size bytes) the bad
// upload is aborted and a non-nil error is returned so the caller falls back to
// a fresh upload. On a mid-resume transfer error the upload is left intact so
// the next attempt can resume again.
func (b *S3Blob) tryResumeUpload(
	ctx context.Context,
	file *os.File,
	fullKey string,
	size int64,
	start time.Time,
) (*TransferInfo, error) {
	_, span := trace.Start(ctx, "S3Blob.tryResumeUpload")
	defer span.End()

	if b.mpu == nil {
		return nil, errNoResumableUpload
	}

	uploadID, err := b.findMultipartUpload(ctx, fullKey)
	if err != nil {
		return nil, err
	}
	slog.Debug("found resumable multipart upload", "key", fullKey, "upload_id", uploadID)

	parts, err := b.listUploadedParts(ctx, fullKey, uploadID)
	if err != nil {
		return nil, fmt.Errorf("listing parts of upload %s: %w", uploadID, err)
	}

	if len(parts) == 0 {
		// No parts means the part size is unknown, so this upload cannot be
		// resumed: abort it and start over.
		b.abandonUpload(ctx, fullKey, uploadID)
		return nil, fmt.Errorf("multipart upload %s has no parts: %w", uploadID, errStaleUploadAbandoned)
	}

	partSize := aws.ToInt64(parts[0].Size)
	expectedPartCount := (size + partSize - 1) / partSize

	if !partsFormResumablePrefix(parts, partSize, expectedPartCount, size) {
		// Gaps/duplicates or sizes inconsistent with size bytes mean this
		// orphan can never resume into the content being saved. Abort it.
		b.abandonUpload(ctx, fullKey, uploadID)
		return nil, fmt.Errorf("multipart upload %s layout does not match %d-byte content: %w", uploadID, size, errStaleUploadAbandoned)
	}

	completed := make([]types.CompletedPart, 0, expectedPartCount)
	var transferred int64

	for _, p := range parts {
		completed = append(completed, types.CompletedPart{
			ETag:       p.ETag,
			PartNumber: p.PartNumber,
		})
	}

	// Upload every still-missing part from its offset in the local file.
	for partNumber := int64(len(parts) + 1); partNumber <= expectedPartCount; partNumber++ {
		offset := (partNumber - 1) * partSize
		length := min(partSize, size-offset)

		out, err := b.mpu.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(b.bucketName),
			Key:        aws.String(fullKey),
			UploadId:   aws.String(uploadID),
			PartNumber: aws.Int32(int32(partNumber)),
			Body:       io.NewSectionReader(file, offset, length),
		})
		if err != nil {
			// Leave the upload (parts 1..partNumber-1 are still good) for the
			// next attempt; the caller falls back for now.
			return nil, fmt.Errorf("uploading part %d of %s: %w", partNumber, uploadID, err)
		}
		transferred += length
		completed = append(completed, types.CompletedPart{
			ETag:       out.ETag,
			PartNumber: aws.Int32(int32(partNumber)),
		})
	}

	sort.Slice(completed, func(i, j int) bool {
		return aws.ToInt32(completed[i].PartNumber) < aws.ToInt32(completed[j].PartNumber)
	})

	if _, err := b.mpu.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(b.bucketName),
		Key:      aws.String(fullKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completed,
		},
	}); err != nil {
		return nil, fmt.Errorf("completing multipart upload %s: %w", uploadID, err)
	}

	duration := time.Since(start)
	var speed float64
	if transferred > 0 {
		speed = calculateTransferSpeedMBps(transferred, duration)
	}

	span.SetAttributes(
		attribute.Int64("bytes_transferred", transferred),
		attribute.String("transfer_speed", fmt.Sprintf("%.2fMB/s", speed)),
		attribute.String("request_id", uploadID),
		attribute.Int64("part_count", expectedPartCount),
	)

	return &TransferInfo{
		BytesTransferred: transferred,
		TransferSpeed:    speed,
		RequestID:        uploadID,
		Duration:         duration,
		PartCount:        int(expectedPartCount),
		Concurrency:      1, // missing parts are uploaded serially during resume
		Resumed:          true,
	}, nil
}

// partsFormResumablePrefix validates that parts are exactly the contiguous
// prefix 1..len(parts) and each part's stored size is the size it must have if
// the upload assembles into size bytes with partSize-sized parts.
func partsFormResumablePrefix(parts []types.Part, partSize, expectedPartCount, size int64) bool {
	if partSize <= 0 || int64(len(parts)) > expectedPartCount {
		return false
	}
	for i, p := range parts {
		partNumber := int64(i + 1)
		if aws.ToInt32(p.PartNumber) != int32(partNumber) {
			return false
		}
		want := partSize
		if partNumber == expectedPartCount {
			want = size - (expectedPartCount-1)*partSize
		}
		if aws.ToInt64(p.Size) != want || want <= 0 {
			return false
		}
	}
	return true
}

// findMultipartUpload returns the upload ID of an in-progress multipart upload
// for fullKey (exact match), or errNoResumableUpload if none exists.
func (b *S3Blob) findMultipartUpload(ctx context.Context, fullKey string) (string, error) {
	input := &s3.ListMultipartUploadsInput{
		Bucket: aws.String(b.bucketName),
		Prefix: aws.String(fullKey),
	}
	for {
		out, err := b.mpu.ListMultipartUploads(ctx, input)
		if err != nil {
			return "", fmt.Errorf("listing multipart uploads: %w", err)
		}
		for _, u := range out.Uploads {
			if aws.ToString(u.Key) == fullKey {
				return aws.ToString(u.UploadId), nil
			}
		}
		if !aws.ToBool(out.IsTruncated) {
			return "", errNoResumableUpload
		}
		input.KeyMarker = out.NextKeyMarker
		input.UploadIdMarker = out.NextUploadIdMarker
	}
}

// listUploadedParts lists every stored part of uploadID, following pagination.
func (b *S3Blob) listUploadedParts(ctx context.Context, fullKey, uploadID string) ([]types.Part, error) {
	var parts []types.Part
	input := &s3.ListPartsInput{
		Bucket:   aws.String(b.bucketName),
		Key:      aws.String(fullKey),
		UploadId: aws.String(uploadID),
	}
	for {
		out, err := b.mpu.ListParts(ctx, input)
		if err != nil {
			return nil, err
		}
		parts = append(parts, out.Parts...)
		if !aws.ToBool(out.IsTruncated) {
			sort.Slice(parts, func(i, j int) bool {
				return aws.ToInt32(parts[i].PartNumber) < aws.ToInt32(parts[j].PartNumber)
			})
			return parts, nil
		}
		input.PartNumberMarker = out.NextPartNumberMarker
	}
}

// failedUploadAbortTimeout bounds the independent abort attempt performed
// after an upload fails, so it cannot stall job shutdown.
var failedUploadAbortTimeout = 10 * time.Second

// bestEffortAbortForKey discovers and aborts any in-progress multipart upload
// still sitting on fullKey after an upload failure.
//
// The discovery uses a context independent of the call context — when the
// upload failed because that context was cancelled, the SDK uploader's own
// abort (made with the same context) can fail and leave the multipart upload
// behind. Best-effort: every failure is only logged; it never changes the
// upload error the caller received.
func (b *S3Blob) bestEffortAbortForKey(ctx context.Context, fullKey string) {
	if b.mpu == nil {
		return
	}
	bgCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), failedUploadAbortTimeout)
	defer cancel()

	uploadID, err := b.findMultipartUpload(bgCtx, fullKey)
	switch {
	case errors.Is(err, errNoResumableUpload):
		return
	case err != nil:
		slog.Warn("failed to discover multipart upload for best-effort abort",
			"key", fullKey, "error", err)
		return
	}
	if err := b.abortUpload(bgCtx, fullKey, uploadID); err != nil {
		slog.Warn("best-effort abort of failed upload failed",
			"key", fullKey, "upload_id", uploadID, "error", err)
	} else {
		slog.Debug("aborted multipart upload left by failed upload",
			"key", fullKey, "upload_id", uploadID)
	}
}

// abandonUpload aborts a non-resumable incomplete upload. The decision is
// reported at Info level when the abort succeeds (Warning when it does not), so
// abandoning an upload is observable in the job log just like resuming one.
func (b *S3Blob) abandonUpload(ctx context.Context, fullKey, uploadID string) {
	if err := b.abortUpload(ctx, fullKey, uploadID); err != nil {
		slog.Warn("failed to abort non-resumable multipart upload",
			"key", fullKey, "upload_id", uploadID, "error", err)
		return
	}
	slog.Info("aborted an incomplete multipart upload that could not be resumed, uploading again",
		"key", fullKey, "upload_id", uploadID)
}

// abortUpload aborts uploadID on fullKey (already including the bucket prefix).
func (b *S3Blob) abortUpload(ctx context.Context, fullKey, uploadID string) error {
	if b.mpu == nil || uploadID == "" {
		return nil
	}
	_, err := b.mpu.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(b.bucketName),
		Key:      aws.String(fullKey),
		UploadId: aws.String(uploadID),
	})
	if err != nil {
		return fmt.Errorf("aborting multipart upload %s: %w", uploadID, err)
	}
	return nil
}

// AbortUpload aborts an in-progress multipart upload. It implements
// MultipartAborter. A nil upload store or empty upload ID is a no-op.
func (b *S3Blob) AbortUpload(ctx context.Context, key, uploadID string) error {
	return b.abortUpload(ctx, b.getFullKey(key), uploadID)
}
