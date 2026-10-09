package store

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// fakeMPU models one in-progress (or completed/aborted) multipart upload.
type fakeMPU struct {
	key       string
	parts     map[int32][]byte
	aborted   bool
	completed bool
}

// fakeS3Server speaks just enough of the S3 XML/HTTP protocol to exercise
// multipart discovery, resume, abort and the manager-Upload fallback.
type fakeS3Server struct {
	t      *testing.T
	mu     sync.Mutex
	server *httptest.Server

	uploads map[string]*fakeMPU
	objects map[string][]byte
	nextID  int

	listUploadsCalls int
	partPuts         []int32
	abortCalls       []string

	denyListUploads bool
	denyListParts   bool
	denyUploadPart  map[string]bool

	// denyPartFrom, when non-zero, denies UploadPart for part numbers >= it
	// (regardless of upload ID). 0 = off.
	denyPartFrom int32
	// failAbortRemaining is the number of AbortMultipartUpload calls that
	// fail with 500 before aborts start succeeding.
	failAbortRemaining int
	// denyAbortRemaining is the number of AbortMultipartUpload calls denied
	// (403, non-retryable) before aborts start succeeding.
	denyAbortRemaining int
}

func newFakeS3Server(t *testing.T) *fakeS3Server {
	t.Helper()
	s := &fakeS3Server{
		t:              t,
		uploads:        map[string]*fakeMPU{},
		objects:        map[string][]byte{},
		denyUploadPart: map[string]bool{},
	}
	s.server = httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(s.server.Close)
	return s
}

func (s *fakeS3Server) resetPartPuts() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partPuts = nil
}

func (s *fakeS3Server) writeError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

func (s *fakeS3Server) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pathParts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	key := strings.Join(pathParts[1:], "/") // pathParts[0] is the bucket
	q := r.URL.Query()

	switch {
	case q.Has("uploads") && r.Method == http.MethodPost:
		// InitiateMultipartUpload
		s.nextID++
		id := fmt.Sprintf("upload-%d", s.nextID)
		s.uploads[id] = &fakeMPU{key: key, parts: map[int32][]byte{}}

		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Bucket>bucket</Bucket><Key>%s</Key><UploadId>%s</UploadId>
</InitiateMultipartUploadResult>`, key, id)

	case q.Has("uploads") && r.Method == http.MethodGet:
		// ListMultipartUploads
		s.listUploadsCalls++
		if s.denyListUploads {
			s.writeError(w, http.StatusForbidden, "AccessDenied")
			return
		}
		prefix := q.Get("prefix")
		var ids []string
		for id, mpu := range s.uploads {
			if !mpu.aborted && !mpu.completed && strings.HasPrefix(mpu.key, prefix) {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		var b bytes.Buffer
		b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Bucket>bucket</Bucket><IsTruncated>false</IsTruncated>`)
		for _, id := range ids {
			fmt.Fprintf(&b, `<Upload><Key>%s</Key><UploadId>%s</UploadId>
<Initiated>2026-01-01T00:00:00.000Z</Initiated><StorageClass>STANDARD</StorageClass></Upload>`,
				s.uploads[id].key, id)
		}
		b.WriteString(`</ListMultipartUploadsResult>`)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write(b.Bytes())

	case q.Has("uploadId") && r.Method == http.MethodPut:
		// UploadPart
		id := q.Get("uploadId")
		n, err := strconv.Atoi(q.Get("partNumber"))
		if err != nil {
			s.t.Fatalf("bad part number: %v", err)
		}
		if s.denyUploadPart[id] || s.denyPartFrom != 0 && int32(n) >= s.denyPartFrom {
			s.writeError(w, http.StatusForbidden, "AccessDenied")
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			s.t.Fatalf("read part body: %v", err)
		}
		mpu := s.uploads[id]
		mpu.parts[int32(n)] = body
		s.partPuts = append(s.partPuts, int32(n))
		w.Header().Set("ETag", fmt.Sprintf(`"etag-%s-%d"`, id, n))

	case q.Has("uploadId") && r.Method == http.MethodPost:
		// CompleteMultipartUpload
		id := q.Get("uploadId")
		mpu := s.uploads[id]
		var req struct {
			Parts []struct {
				PartNumber int32 `xml:"PartNumber"`
			} `xml:"Part"`
		}
		if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
			s.t.Fatalf("decode complete body: %v", err)
		}
		var assembled bytes.Buffer
		for _, p := range req.Parts {
			assembled.Write(mpu.parts[p.PartNumber])
		}
		s.objects[mpu.key] = assembled.Bytes()
		mpu.completed = true
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Bucket>bucket</Bucket><Key>%s</Key><ETag>"final-etag"</ETag></CompleteMultipartUploadResult>`, mpu.key)

	case q.Has("uploadId") && r.Method == http.MethodGet:
		// ListParts
		id := q.Get("uploadId")
		if s.denyListParts {
			s.writeError(w, http.StatusForbidden, "AccessDenied")
			return
		}
		mpu := s.uploads[id]
		var nums []int32
		for n := range mpu.parts {
			nums = append(nums, n)
		}
		var b bytes.Buffer
		b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Bucket>bucket</Bucket><Key>` + mpu.key + `</Key><UploadId>` + id + `</UploadId>
<IsTruncated>false</IsTruncated>`)
		sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
		for _, n := range nums {
			fmt.Fprintf(&b, `<Part><PartNumber>%d</PartNumber>
<LastModified>2026-01-01T00:00:00.000Z</LastModified>
<ETag>"etag-%s-%d"</ETag><Size>%d</Size></Part>`, n, id, n, len(mpu.parts[n]))
		}
		b.WriteString(`</ListPartsResult>`)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write(b.Bytes())

	case q.Has("uploadId") && r.Method == http.MethodDelete:
		// AbortMultipartUpload
		id := q.Get("uploadId")
		if s.failAbortRemaining > 0 {
			s.failAbortRemaining--
			s.writeError(w, http.StatusInternalServerError, "InternalError")
			return
		}
		if s.denyAbortRemaining > 0 {
			s.denyAbortRemaining--
			s.writeError(w, http.StatusForbidden, "AccessDenied")
			return
		}
		s.abortCalls = append(s.abortCalls, id)
		mpu := s.uploads[id]
		mpu.aborted = true
		w.WriteHeader(http.StatusNoContent)

	case r.Method == http.MethodPut:
		// PutObject (small files / manager fallback)
		body, _ := io.ReadAll(r.Body)
		s.objects[key] = body
		w.Header().Set("ETag", `"put-etag"`)

	case r.Method == http.MethodHead:
		obj, ok := s.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(obj)))

	default:
		obj, ok := s.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(obj)
	}
}

// newResumeTestBlob builds an S3Blob pointed at the fake server.
func newResumeTestBlob(t *testing.T, s *fakeS3Server) (*S3Blob, *s3.Client) {
	t.Helper()
	s3Client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(s.server.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("ak", "sk", ""),
	})
	return &S3Blob{
		client:              s3Client,
		header:              s3Client,
		mpu:                 s3Client,
		uploader:            manager.NewUploader(s3Client),
		downloader:          manager.NewDownloader(s3Client),
		bucketName:          "bucket",
		uploadConcurrency:   manager.DefaultUploadConcurrency,
		downloadConcurrency: defaultDownloadConcurrency,
	}, s3Client
}

func writeSourceFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func getObject(t *testing.T, ctx context.Context, client *s3.Client, key string) []byte {
	t.Helper()
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("get object %q: %v", key, err)
	}
	defer func() { _ = out.Body.Close() }()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// writeLargeSourceFile writes a file holding exactly size bytes and returns
// its path and content.
func writeLargeSourceFile(t *testing.T, size int64) (string, []byte) {
	t.Helper()
	content := bytes.Repeat([]byte("0123456789"), int(size/10))
	path := filepath.Join(t.TempDir(), "large-source")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return path, content
}

const fiveMiB = 5 << 20

// TestS3FailedUploadBestEffortAbort covers TR-4.1/4.2: when the SDK's own
// abort fails, an independent abort is attempted with the same upload ID; the
// error Upload returns is always the original upload error.
func TestS3FailedUploadBestEffortAbort(t *testing.T) {
	ctx := context.Background()

	t.Run("independent abort succeeds after SDK abort fails", func(t *testing.T) {
		srv := newFakeS3Server(t)
		blob, s3Client := newResumeTestBlob(t, srv)
		blob.uploader = manager.NewUploader(s3Client, func(u *manager.Uploader) {
			u.PartSize = fiveMiB
			u.Concurrency = 1
		})

		srcPath, _ := writeLargeSourceFile(t, 2*fiveMiB) // 10 MiB → 2 parts
		srv.denyPartFrom = 2
		srv.denyAbortRemaining = 1 // only the SDK's abort is denied

		_, err := blob.Upload(ctx, srcPath, "abort-key", 0)
		if err == nil {
			t.Fatal("expected upload error")
		}
		if !strings.Contains(err.Error(), "AccessDenied") || strings.Contains(err.Error(), "abort") {
			t.Errorf("Upload error = %v, want the original AccessDenied error, not an abort error", err)
		}
		if len(srv.abortCalls) != 1 {
			t.Fatalf("successful abort calls = %d, want 1", len(srv.abortCalls))
		}
		uploadID := srv.abortCalls[0]
		if !srv.uploads[uploadID].aborted {
			t.Error("orphaned multipart upload was not aborted")
		}
	})

	t.Run("abort failure leaves upload error unchanged", func(t *testing.T) {
		srv := newFakeS3Server(t)
		blob, s3Client := newResumeTestBlob(t, srv)
		blob.uploader = manager.NewUploader(s3Client, func(u *manager.Uploader) {
			u.PartSize = fiveMiB
			u.Concurrency = 1
		})

		srcPath, _ := writeLargeSourceFile(t, 2*fiveMiB)
		srv.denyPartFrom = 2
		srv.denyAbortRemaining = 2 // SDK abort + best-effort abort both denied

		_, err := blob.Upload(ctx, srcPath, "abort-key-2", 0)
		if err == nil || !strings.Contains(err.Error(), "AccessDenied") {
			t.Errorf("Upload error = %v, want AccessDenied", err)
		}
		if len(srv.abortCalls) != 0 {
			t.Errorf("no successful aborts expected, got %d", len(srv.abortCalls))
		}
		for id, mpu := range srv.uploads {
			if mpu.aborted {
				t.Errorf("upload %s unexpectedly aborted", id)
			}
		}
	})
}

// TestBestEffortAbortIndependentContext covers TR-4.1: an already-cancelled
// call context must not prevent the orphaned upload from being aborted.
func TestBestEffortAbortIndependentContext(t *testing.T) {
	ctx := context.Background()
	srv := newFakeS3Server(t)
	blob, s3Client := newResumeTestBlob(t, srv)

	const key = "ctx-key"
	created, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String("bucket"), Key: aws.String(key),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String("bucket"), Key: aws.String(key), UploadId: created.UploadId,
		PartNumber: aws.Int32(1), Body: strings.NewReader("xxxxx"),
	}); err != nil {
		t.Fatal(err)
	}
	uploadID := aws.ToString(created.UploadId)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	blob.bestEffortAbortForKey(cancelled, key)

	if !srv.uploads[uploadID].aborted {
		t.Error("orphan was not aborted despite independent context")
	}
}

// TestS3FailedUploadResumedNextSave covers TR-4.3: an upload failure whose
// abort never succeeds leaves an orphan that the next save resumes.
func TestS3FailedUploadResumedNextSave(t *testing.T) {
	ctx := context.Background()
	srv := newFakeS3Server(t)
	blob, s3Client := newResumeTestBlob(t, srv)
	blob.uploader = manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.PartSize = fiveMiB
		u.Concurrency = 1
	})

	srcPath, content := writeLargeSourceFile(t, 2*fiveMiB)
	const key = "heal-key"

	srv.denyPartFrom = 2
	srv.denyAbortRemaining = 2
	if _, err := blob.Upload(ctx, srcPath, key, 0); err == nil {
		t.Fatal("expected first upload to fail")
	}
	var orphanID string
	for id, mpu := range srv.uploads {
		if mpu.key == key && !mpu.aborted {
			orphanID = id
		}
	}
	if orphanID == "" {
		t.Fatal("expected an orphaned multipart upload to remain")
	}

	// Simulate a restart: clear the injected failures, upload again.
	srv.denyPartFrom = 0
	srv.denyAbortRemaining = 0
	info, err := blob.Upload(ctx, srcPath, key, 0)
	if err != nil {
		t.Fatalf("second Upload: %v", err)
	}
	if !srv.uploads[orphanID].completed {
		t.Error("expected the orphaned upload to be resumed and completed")
	}
	if info.BytesTransferred != fiveMiB {
		t.Errorf("BytesTransferred = %d, want only the missing 5MiB part", info.BytesTransferred)
	}
	if got := getObject(t, ctx, s3Client, key); !bytes.Equal(got, content) {
		t.Errorf("resumed object content mismatch (len got=%d want=%d)", len(got), len(content))
	}
}

// TestS3AbandonedSentinel covers TR-I-1.1: a non-resumable incomplete upload
// (bad layout or zero parts) returns errStaleUploadAbandoned — distinct from
// errNoResumableUpload — and is aborted.
func TestS3AbandonedSentinel(t *testing.T) {
	ctx := context.Background()
	content := "0123456789012345678901234" // 25 bytes

	t.Run("bad layout", func(t *testing.T) {
		srv := newFakeS3Server(t)
		blob, s3Client := newResumeTestBlob(t, srv)

		created, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String("bucket"), Key: aws.String("bad-layout-sentinel"),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String("bucket"), Key: aws.String("bad-layout-sentinel"), UploadId: created.UploadId,
			PartNumber: aws.Int32(1), Body: strings.NewReader(content[:7]),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String("bucket"), Key: aws.String("bad-layout-sentinel"), UploadId: created.UploadId,
			PartNumber: aws.Int32(2), Body: strings.NewReader("XXXXXXXXXX"),
		}); err != nil {
			t.Fatal(err)
		}

		src, err := os.Open(writeSourceFile(t, content))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = src.Close() }()

		_, err = blob.tryResumeUpload(ctx, src, "bad-layout-sentinel", int64(len(content)), time.Now())
		if !errors.Is(err, errStaleUploadAbandoned) || errors.Is(err, errNoResumableUpload) {
			t.Fatalf("err = %v, want errStaleUploadAbandoned only", err)
		}
		if !srv.uploads[aws.ToString(created.UploadId)].aborted {
			t.Error("expected the non-resumable upload to be aborted")
		}
	})

	t.Run("zero parts", func(t *testing.T) {
		srv := newFakeS3Server(t)
		blob, s3Client := newResumeTestBlob(t, srv)

		created, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String("bucket"), Key: aws.String("zero-parts-sentinel"),
		})
		if err != nil {
			t.Fatal(err)
		}

		src, err := os.Open(writeSourceFile(t, content))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = src.Close() }()

		_, err = blob.tryResumeUpload(ctx, src, "zero-parts-sentinel", int64(len(content)), time.Now())
		if !errors.Is(err, errStaleUploadAbandoned) {
			t.Fatalf("err = %v, want errStaleUploadAbandoned", err)
		}
		if !srv.uploads[aws.ToString(created.UploadId)].aborted {
			t.Error("expected the zero-part upload to be aborted")
		}
	})
}

// TR-1.1: a partial upload is resumed — existing parts are not re-sent, only
// the missing part is uploaded, and the assembled bytes match the source.
func TestS3ResumeUpload(t *testing.T) {
	ctx := context.Background()
	srv := newFakeS3Server(t)
	blob, s3Client := newResumeTestBlob(t, srv)

	content := "0123456789012345678901234" // 25 bytes
	if len(content) != 25 {
		t.Fatal("test content must be 25 bytes")
	}
	srcPath := writeSourceFile(t, content)
	const objectKey = "resume-key"

	// Simulate a crashed save: MPU created, parts 1 and 2 uploaded.
	created, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID := aws.ToString(created.UploadId)
	if _, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String("bucket"), Key: aws.String(objectKey), UploadId: created.UploadId,
		PartNumber: aws.Int32(1), Body: strings.NewReader(content[:10]),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String("bucket"), Key: aws.String(objectKey), UploadId: created.UploadId,
		PartNumber: aws.Int32(2), Body: strings.NewReader(content[10:20]),
	}); err != nil {
		t.Fatal(err)
	}

	srv.resetPartPuts()
	info, err := blob.Upload(ctx, srcPath, objectKey, 0)
	if err != nil {
		t.Fatalf("Upload resume: %v", err)
	}

	if got, want := srv.partPuts, []int32{3}; len(got) != 1 || got[0] != want[0] {
		t.Errorf("parts uploaded during resume = %v, want only [3]", got)
	}
	if info.PartCount != 3 {
		t.Errorf("PartCount = %d, want 3", info.PartCount)
	}
	if info.BytesTransferred != 5 {
		t.Errorf("BytesTransferred = %d, want 5", info.BytesTransferred)
	}
	if got := string(getObject(t, ctx, s3Client, objectKey)); got != content {
		t.Errorf("object content = %q, want %q", got, content)
	}
	if !srv.uploads[uploadID].completed {
		t.Error("resumed upload was not completed")
	}
}

// TR-1.2: with no in-progress upload, Upload behaves exactly as before
// (discovery happens once, then the manager uploads a fresh object).
func TestS3UploadNoMPU(t *testing.T) {
	ctx := context.Background()
	srv := newFakeS3Server(t)
	blob, s3Client := newResumeTestBlob(t, srv)

	content := "fresh upload contents"
	srcPath := writeSourceFile(t, content)
	const objectKey = "fresh-key"

	info, err := blob.Upload(ctx, srcPath, objectKey, 0)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if srv.listUploadsCalls != 1 {
		t.Errorf("ListMultipartUploads calls = %d, want 1", srv.listUploadsCalls)
	}
	if info.BytesTransferred != int64(len(content)) {
		t.Errorf("BytesTransferred = %d, want %d", info.BytesTransferred, len(content))
	}
	if got := string(getObject(t, ctx, s3Client, objectKey)); got != content {
		t.Errorf("object content = %q, want %q", got, content)
	}
}

// TR-1.3: errors at each recovery stage must not block the save; the manager
// fallback completes a correct object.
func TestS3ResumeFailuresFallback(t *testing.T) {
	ctx := context.Background()
	content := "0123456789012345678901234"

	t.Run("list uploads denied", func(t *testing.T) {
		srv := newFakeS3Server(t)
		srv.denyListUploads = true
		blob, s3Client := newResumeTestBlob(t, srv)

		info, err := blob.Upload(ctx, writeSourceFile(t, content), "k1", 0)
		if err != nil {
			t.Fatalf("Upload: %v", err)
		}
		if info.BytesTransferred != int64(len(content)) {
			t.Errorf("BytesTransferred = %d, want %d", info.BytesTransferred, len(content))
		}
		if got := string(getObject(t, ctx, s3Client, "k1")); got != content {
			t.Errorf("object = %q, want %q", got, content)
		}
	})

	t.Run("list parts denied", func(t *testing.T) {
		srv := newFakeS3Server(t)
		blob, s3Client := newResumeTestBlob(t, srv)

		created, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String("bucket"), Key: aws.String("k2"),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String("bucket"), Key: aws.String("k2"), UploadId: created.UploadId,
			PartNumber: aws.Int32(1), Body: strings.NewReader(content[:10]),
		}); err != nil {
			t.Fatal(err)
		}
		srv.denyListParts = true

		if _, err := blob.Upload(ctx, writeSourceFile(t, content), "k2", 0); err != nil {
			t.Fatalf("Upload: %v", err)
		}
		if got := string(getObject(t, ctx, s3Client, "k2")); got != content {
			t.Errorf("object = %q, want %q", got, content)
		}
	})

	t.Run("missing part upload denied", func(t *testing.T) {
		srv := newFakeS3Server(t)
		blob, s3Client := newResumeTestBlob(t, srv)

		created, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String("bucket"), Key: aws.String("k3"),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String("bucket"), Key: aws.String("k3"), UploadId: created.UploadId,
			PartNumber: aws.Int32(1), Body: strings.NewReader(content[:10]),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String("bucket"), Key: aws.String("k3"), UploadId: created.UploadId,
			PartNumber: aws.Int32(2), Body: strings.NewReader(content[10:20]),
		}); err != nil {
			t.Fatal(err)
		}
		srv.denyUploadPart[aws.ToString(created.UploadId)] = true

		if _, err := blob.Upload(ctx, writeSourceFile(t, content), "k3", 0); err != nil {
			t.Fatalf("Upload: %v", err)
		}
		if got := string(getObject(t, ctx, s3Client, "k3")); got != content {
			t.Errorf("object = %q, want %q", got, content)
		}
	})
}

// TR-1.4: an orphan whose stored layout can't assemble into the source bytes
// is aborted and re-uploaded.
func TestS3ResumeBadLayoutAborted(t *testing.T) {
	ctx := context.Background()
	srv := newFakeS3Server(t)
	blob, s3Client := newResumeTestBlob(t, srv)

	content := "0123456789012345678901234" // 25 bytes

	created, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String("bucket"), Key: aws.String("k4"),
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID := aws.ToString(created.UploadId)
	// Part 1 = 7 bytes, part 2 = 10 bytes: inconsistent with any part size.
	if _, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String("bucket"), Key: aws.String("k4"), UploadId: created.UploadId,
		PartNumber: aws.Int32(1), Body: strings.NewReader(content[:7]),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String("bucket"), Key: aws.String("k4"), UploadId: created.UploadId,
		PartNumber: aws.Int32(2), Body: strings.NewReader("XXXXXXXXXX"),
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := blob.Upload(ctx, writeSourceFile(t, content), "k4", 0); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if !srv.uploads[uploadID].aborted {
		t.Error("expected non-resumable upload to be aborted")
	}
	if got := string(getObject(t, ctx, s3Client, "k4")); got != content {
		t.Errorf("object = %q, want %q", got, content)
	}
}

// An MPU with zero parts (process died immediately after initiation) is aborted
// and the content uploaded fresh.
func TestS3ResumeZeroPartsAborted(t *testing.T) {
	ctx := context.Background()
	srv := newFakeS3Server(t)
	blob, s3Client := newResumeTestBlob(t, srv)

	content := "tiny"
	created, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String("bucket"), Key: aws.String("k5"),
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID := aws.ToString(created.UploadId)

	if _, err := blob.Upload(ctx, writeSourceFile(t, content), "k5", 0); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if !srv.uploads[uploadID].aborted {
		t.Error("expected zero-part upload to be aborted")
	}
	if got := string(getObject(t, ctx, s3Client, "k5")); got != content {
		t.Errorf("object = %q, want %q", got, content)
	}
}
