package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildkite/agent/v4/api"
	"github.com/buildkite/agent/v4/internal/cache/archive"
	"github.com/buildkite/agent/v4/internal/cache/configuration"
	"github.com/klauspost/compress/zip"
)

const (
	rcName    = "test-cache"
	rcDir     = "cached"
	rcFile    = "cached/data.txt"
	rcVal     = "cached file contents"
	rcKeyPart = "resumable-key"
)

// newResumableClient builds a cache client backed by a mock registry and a
// file:// storage directory, chdir-ed into a fresh workspace holding the
// cache target. Returns the client, mock and absolute storage directory.
func newResumableClient(t *testing.T) (*client, *mockAPIClient, string) {
	t.Helper()
	t.Chdir(t.TempDir())

	if err := os.MkdirAll(rcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rcFile, []byte(rcVal), 0o600); err != nil {
		t.Fatal(err)
	}

	storageDir := t.TempDir()
	urlPath := filepath.ToSlash(storageDir)
	if !strings.HasPrefix(urlPath, "/") {
		urlPath = "/" + urlPath
	}

	mock := newMockAPIClient("local_file")
	c := &client{
		api:       mock,
		bucketURL: "file://" + urlPath,
		format:    "zip",
		platform:  "linux/amd64",
		registry:  "~",
		caches: []configuration.Cache{{
			Name:        rcName,
			CacheKey:    []configuration.KeyPart{{Source: configuration.SourceLiteral, Arg: rcKeyPart}},
			TargetPaths: []string{rcDir},
		}},
		suspectBlobs: map[string]struct{}{},
	}
	return c, mock, storageDir
}

func rcAddr() string {
	return cacheAddr([]string{rcDir}, literalKey(rcKeyPart))
}

func literalKey(val string) []api.CacheKeyPart {
	return []api.CacheKeyPart{{Value: val, Mandatory: true}}
}

// newResumableClientWithStorage builds a fresh (restart-like) client with an
// empty mock registry that uses an existing storage directory. It assumes the
// test is already chdir-ed in the workspace holding the cache target.
func newResumableClientWithStorage(t *testing.T, storageDir string) (*client, *mockAPIClient) {
	t.Helper()
	urlPath := filepath.ToSlash(storageDir)
	if !strings.HasPrefix(urlPath, "/") {
		urlPath = "/" + urlPath
	}
	mock := newMockAPIClient("local_file")
	c := &client{
		api:       mock,
		bucketURL: "file://" + urlPath,
		format:    "zip",
		platform:  "linux/amd64",
		registry:  "~",
		caches: []configuration.Cache{{
			Name:        rcName,
			CacheKey:    []configuration.KeyPart{{Source: configuration.SourceLiteral, Arg: rcKeyPart}},
			TargetPaths: []string{rcDir},
		}},
		suspectBlobs: map[string]struct{}{},
	}
	return c, mock
}

// seedCommittedEntry inserts a committed entry directly into the mock registry.
func seedCommittedEntry(t *testing.T, mock *mockAPIClient, blobs []api.CacheBlob, uploadID string) {
	t.Helper()
	mock.registries["~"].cache[rcAddr()] = &mockCacheEntry{
		targetPaths: []string{rcDir},
		cacheKey:    literalKey(rcKeyPart),
		blobs:       blobs,
		committed:   true,
		expiresAt:   time.Now().Add(24 * time.Hour),
		uploadID:    uploadID,
	}
}

// assertRestoreHit removes the target and asserts restore hits with the
// expected content.
func assertRestoreHit(t *testing.T, ctx context.Context, c *client) {
	t.Helper()
	if err := os.RemoveAll(rcDir); err != nil {
		t.Fatal(err)
	}
	res, err := c.Restore(ctx, rcName)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !res.CacheRestored || !res.CacheHit || res.FallbackUsed {
		t.Fatalf("want exact hit, got %+v", res)
	}
	got, err := os.ReadFile(rcFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != rcVal {
		t.Errorf("restored content = %q, want %q", got, rcVal)
	}
}

func assertMiss(t *testing.T, ctx context.Context, c *client, reasonPart string) RestoreResult {
	t.Helper()
	res, err := c.Restore(ctx, rcName)
	if err != nil {
		t.Fatalf("Restore should degrade to miss, not error: %v", err)
	}
	if res.CacheRestored || res.CacheHit {
		t.Fatalf("want miss, got %+v", res)
	}
	if reasonPart != "" && !strings.Contains(res.NotRestoredReason, reasonPart) {
		t.Errorf("NotRestoredReason = %q, want substring %q", res.NotRestoredReason, reasonPart)
	}
	return res
}

// targetUntouched asserts a pre-mutation failure left the original target.
func targetUntouched(t *testing.T) {
	t.Helper()
	got, err := os.ReadFile(rcFile)
	if err != nil {
		t.Fatalf("target should be untouched: %v", err)
	}
	if string(got) != rcVal {
		t.Errorf("target content = %q, want %q", got, rcVal)
	}
}

// TestResumableSuccess covers TR-7.1/AC-1: save commits and a later restore
// round-trips the content.
func TestResumableSuccess(t *testing.T) {
	ctx := context.Background()
	c, mock, _ := newResumableClient(t)

	saved, err := c.Save(ctx, rcName)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if !saved.CacheEntryCreated || saved.UploadSkipped || saved.UploadID == "" {
		t.Errorf("bad save result: %+v", saved)
	}
	if !mock.registries["~"].cache[rcAddr()].committed {
		t.Error("entry not committed")
	}
	assertRestoreHit(t, ctx, c)
}

// TestResumablePartialUploadFailure covers TR-7.2/AC-2: a failed upload leaves
// no committed entry; restore misses; a later save heals and restore hits.
func TestResumablePartialUploadFailure(t *testing.T) {
	ctx := context.Background()
	c, mock, storageDir := newResumableClient(t)

	// Make the storage directory read-only so the blob upload fails.
	if err := os.Chmod(storageDir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(storageDir, 0o755) })

	if _, err := c.Save(ctx, rcName); err == nil {
		t.Fatal("expected save to fail when upload fails")
	}

	entry := mock.registries["~"].cache[rcAddr()]
	if entry == nil || entry.committed {
		t.Fatal("failed save must leave an uncommitted (or no) entry, never a committed one")
	}

	// Restore cannot hit the uncommitted entry.
	res, err := c.Restore(ctx, rcName)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if res.CacheRestored {
		t.Error("uncommitted entry must not be restored")
	}

	// Heal: writable storage, save again.
	if err := os.Chmod(storageDir, 0o755); err != nil {
		t.Fatal(err)
	}
	saved, err := c.Save(ctx, rcName)
	if err != nil {
		t.Fatalf("second Save: %v", err)
	}
	if !saved.CacheEntryCreated {
		t.Error("re-save should create the entry")
	}
	assertRestoreHit(t, ctx, c)
	if len(mock.commitCalls) != 1 {
		t.Errorf("commits = %d, want exactly 1", len(mock.commitCalls))
	}
}

// TestResumableSuspectBlob covers TR-5.1/5.4/AC-7: after restore observes a
// digest mismatch, a later same-process save must upload (not skip on size).
func TestResumableSuspectBlob(t *testing.T) {
	ctx := context.Background()
	c, _, storageDir := newResumableClient(t)

	saved, err := c.Save(ctx, rcName)
	if err != nil {
		t.Fatal(err)
	}
	digest := saved.Archive.Sha256Sum

	// Same-size garbage blob: size-only trust would skip it; suspect guard
	// must not.
	garbage := strings.Repeat("X", int(saved.Archive.Size))
	if err := os.WriteFile(filepath.Join(storageDir, digest), []byte(garbage), 0o600); err != nil {
		t.Fatal(err)
	}

	assertMiss(t, ctx, c, "digest mismatch")
	targetUntouched(t)

	resaved, err := c.Save(ctx, rcName)
	if err != nil {
		t.Fatalf("Save after mismatch: %v", err)
	}
	if !resaved.CacheEntryCreated || resaved.UploadSkipped {
		t.Fatalf("suspect blob must be re-uploaded, got %+v", resaved)
	}
	assertRestoreHit(t, ctx, c)
}

// TestResumableArchiveValidationSuspect covers TR-5.2: an archive that fails
// format validation (with digest verification bypassed) marks the blob
// suspect, and the next save re-uploads.
func TestResumableArchiveValidationSuspect(t *testing.T) {
	ctx := context.Background()
	c, mock, storageDir := newResumableClient(t)

	saved, err := c.Save(ctx, rcName)
	if err != nil {
		t.Fatal(err)
	}
	digest := saved.Archive.Sha256Sum

	// Non-zip garbage + make digest verification skip (unknown algorithm).
	if err := os.WriteFile(filepath.Join(storageDir, digest), []byte("this is not a zip archive at all"), 0o600); err != nil {
		t.Fatal(err)
	}
	entry := mock.registries["~"].cache[rcAddr()]
	entry.blobs[0].Digest.Algorithm = "none"

	assertMiss(t, ctx, c, "archive failed verification")
	if !c.isBlobSuspect(digest) {
		t.Error("blob should be marked suspect after validation failure")
	}

	resaved, err := c.Save(ctx, rcName)
	if err != nil {
		t.Fatal(err)
	}
	if resaved.UploadSkipped {
		t.Error("suspect blob must not skip upload")
	}
	assertRestoreHit(t, ctx, c)
}

// TestResumableMissing covers TR-7.3/AC-5: a pure miss and an entry whose
// blob was deleted both degrade to clean misses and self-heal.
func TestResumableMissing(t *testing.T) {
	ctx := context.Background()

	t.Run("pure miss", func(t *testing.T) {
		c, _, _ := newResumableClient(t)
		assertMiss(t, ctx, c, "")
		targetUntouched(t)
	})

	t.Run("entry without blob heals", func(t *testing.T) {
		c, _, storageDir := newResumableClient(t)
		saved, err := c.Save(ctx, rcName)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(storageDir, saved.Archive.Sha256Sum)); err != nil {
			t.Fatal(err)
		}

		assertMiss(t, ctx, c, "missing blob")
		targetUntouched(t)

		// Re-save uploads again; restore hits.
		resaved, err := c.Save(ctx, rcName)
		if err != nil {
			t.Fatal(err)
		}
		if !resaved.CacheEntryCreated {
			t.Error("re-save should create the entry")
		}
		assertRestoreHit(t, ctx, c)
	})
}

// TestResumableStaleStructure covers TR-6.1/6.2/6.3: a committed entry with no
// blobs or an empty digest is a clean miss + invalidate, then heals.
func TestResumableStaleStructure(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name   string
		mutate func(*mockCacheEntry)
	}{
		{name: "empty blobs", mutate: func(e *mockCacheEntry) { e.blobs = nil }},
		{name: "empty digest value", mutate: func(e *mockCacheEntry) {
			e.blobs[0].Digest.Value = ""
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, mock, _ := newResumableClient(t)
			if _, err := c.Save(ctx, rcName); err != nil {
				t.Fatal(err)
			}
			entry := mock.registries["~"].cache[rcAddr()]
			tc.mutate(entry)

			assertMiss(t, ctx, c, "stale entry structure")

			// Entry was invalidated (deleted from the mock registry).
			if _, ok := mock.registries["~"].cache[rcAddr()]; ok {
				t.Error("stale entry should have been invalidated")
			}

			// Heal: targets still on disk, save and restore.
			resaved, err := c.Save(ctx, rcName)
			if err != nil {
				t.Fatal(err)
			}
			if !resaved.CacheEntryCreated {
				t.Error("re-save should create the entry")
			}
			assertRestoreHit(t, ctx, c)
		})
	}
}

// archiveWithManifestVersion builds an otherwise-valid zip whose manifest has
// the given version, returning its bytes and sha256.
func archiveWithManifestVersion(t *testing.T, version int) ([]byte, string) {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create(archive.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	m := archive.Manifest{
		Version:  version,
		Mappings: map[string]string{rcDir: archive.AnchorCWD},
	}
	if err := json.NewEncoder(w).Encode(m); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(buf.Bytes())
	return buf.Bytes(), hex.EncodeToString(sum[:])
}

// TestResumableOldFormats covers TR-7.3/AC-6: an archive with an old manifest
// version and an old entry with an empty upload ID both miss, then heal.
func TestResumableOldFormats(t *testing.T) {
	ctx := context.Background()

	t.Run("old manifest version", func(t *testing.T) {
		c, _, storageDir := newResumableClient(t)

		oldBytes, oldSum := archiveWithManifestVersion(t, 1)
		if err := os.WriteFile(filepath.Join(storageDir, oldSum), oldBytes, 0o600); err != nil {
			t.Fatal(err)
		}
		seedCommittedEntry(t, c.api.(*mockAPIClient), []api.CacheBlob{{
			Digest:      api.CacheDigest{Algorithm: "sha256", Value: oldSum},
			FileSize:    int64(len(oldBytes)),
			Compression: "zip",
		}}, "old-upload")

		assertMiss(t, ctx, c, "archive failed verification")

		// Re-save current content; restore hits.
		resaved, err := c.Save(ctx, rcName)
		if err != nil {
			t.Fatal(err)
		}
		if !resaved.CacheEntryCreated {
			t.Error("re-save should create the entry")
		}
		assertRestoreHit(t, ctx, c)
	})

	t.Run("old entry empty upload id with missing blob", func(t *testing.T) {
		c, mock, _ := newResumableClient(t)
		seedCommittedEntry(t, mock, []api.CacheBlob{{
			Digest:      api.CacheDigest{Algorithm: "sha256", Value: "missing-blob-digest"},
			FileSize:    10,
			Compression: "zip",
		}}, "") // no upload ID: pre-upload-id registry row

		assertMiss(t, ctx, c, "missing blob")

		if len(mock.expireCalls) != 1 || mock.expireCalls[0].UploadID != "" {
			t.Errorf("expire request = %+v, want unconditional (empty upload ID)", mock.expireCalls)
		}

		resaved, err := c.Save(ctx, rcName)
		if err != nil {
			t.Fatal(err)
		}
		if !resaved.CacheEntryCreated {
			t.Error("re-save should create the entry")
		}
		assertRestoreHit(t, ctx, c)
	})
}

// TestResumableIdempotency covers TR-7.4/AC-8: repeated saves produce no extra
// objects/entries (including a "restart" into a fresh process), and repeated
// restores are identical.
func TestResumableIdempotency(t *testing.T) {
	ctx := context.Background()

	t.Run("repeated saves", func(t *testing.T) {
		c, mock, storageDir := newResumableClient(t)

		first, err := c.Save(ctx, rcName)
		if err != nil {
			t.Fatal(err)
		}
		second, err := c.Save(ctx, rcName)
		if err != nil {
			t.Fatal(err)
		}
		if second.CacheEntryCreated {
			t.Error("second save must be a peek no-op")
		}
		if len(mock.commitCalls) != 1 {
			t.Errorf("commits = %d, want 1", len(mock.commitCalls))
		}
		entries := listDataBlobs(t, storageDir)
		if len(entries) != 1 {
			t.Errorf("storage objects = %v, want exactly 1", entries)
		}

		// Simulate an agent restart: a brand-new client (empty registry)
		// pointed at the same storage directory. Save must dedupe, not
		// re-upload, and commit an entry referencing the same blob digest.
		c2, mock2 := newResumableClientWithStorage(t, storageDir)
		third, err := c2.Save(ctx, rcName)
		if err != nil {
			t.Fatalf("restart Save: %v", err)
		}
		if !third.CacheEntryCreated || !third.UploadSkipped {
			t.Errorf("restart save should dedupe (created, upload skipped), got %+v", third)
		}
		if len(mock2.commitCalls) != 1 {
			t.Errorf("restart commits = %d, want 1", len(mock2.commitCalls))
		}
		if got := listDataBlobs(t, storageDir); len(got) != 1 || got[0] != first.Archive.Sha256Sum {
			t.Errorf("storage = %v, want the single original blob %s", got, first.Archive.Sha256Sum)
		}
		assertRestoreHit(t, ctx, c2)
	})

	t.Run("repeated restores", func(t *testing.T) {
		c, _, _ := newResumableClient(t)
		if _, err := c.Save(ctx, rcName); err != nil {
			t.Fatal(err)
		}
		assertRestoreHit(t, ctx, c)
		assertRestoreHit(t, ctx, c)
	})

	t.Run("repeated misses", func(t *testing.T) {
		c, mock, storageDir := newResumableClient(t)
		saved, err := c.Save(ctx, rcName)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(storageDir, saved.Archive.Sha256Sum)); err != nil {
			t.Fatal(err)
		}
		assertMiss(t, ctx, c, "missing blob")
		// After the first miss invalidated the entry, the second restore is a
		// pure miss.
		res, err := c.Restore(ctx, rcName)
		if err != nil || res.CacheRestored {
			t.Fatalf("second restore want miss, got %+v err %v", res, err)
		}
		_ = mock
	})
}

// listDataBlobs lists data blob names in dir, excluding metadata sidecars.
func listDataBlobs(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".attrs.json") {
			continue
		}
		names = append(names, e.Name())
	}
	return names
}
