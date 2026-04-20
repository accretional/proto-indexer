package embed

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// fakeBinary writes a shell script to tmp that prints fixedJSON and returns its path.
func fakeBinary(t *testing.T, fixedJSON string) string {
	t.Helper()
	script := "#!/bin/sh\necho '" + fixedJSON + "'\n"
	p := filepath.Join(t.TempDir(), "fake-macos-vision")
	if err := os.WriteFile(p, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestAppleProvider_ParsesVector(t *testing.T) {
	const responseJSON = `{"cliVersion":"2","subcommand":"nl","operation":"embed","result":{"vector":[0.1,0.2,0.3,0.4],"dimension":4,"mode":"sentence"}}`
	bin := fakeBinary(t, responseJSON)

	p := NewApple(bin)
	vecs, err := p.Embed(context.Background(), []string{"hello world"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vecs) != 1 {
		t.Fatalf("expected 1 vector, got %d", len(vecs))
	}
	want := []float32{0.1, 0.2, 0.3, 0.4}
	for i, v := range want {
		if vecs[0][i] != v {
			t.Errorf("vec[%d] = %v, want %v", i, vecs[0][i], v)
		}
	}
}

func TestAppleProvider_SkipsEmptyText(t *testing.T) {
	const responseJSON = `{"cliVersion":"2","subcommand":"nl","operation":"embed","result":{"vector":[1,0,0],"dimension":3,"mode":"sentence"}}`
	bin := fakeBinary(t, responseJSON)

	p := NewApple(bin)
	// Second entry is empty — provider should not invoke the binary for it.
	vecs, err := p.Embed(context.Background(), []string{"non-empty", "", "also non-empty"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vecs) != 3 {
		t.Fatalf("expected 3 results, got %d", len(vecs))
	}
	if vecs[0] == nil {
		t.Error("expected non-nil vector for index 0")
	}
	if vecs[1] != nil {
		t.Errorf("expected nil vector for empty text at index 1, got %v", vecs[1])
	}
	if vecs[2] == nil {
		t.Error("expected non-nil vector for index 2")
	}
}

func TestAppleProvider_ErrorOnBadJSON(t *testing.T) {
	bin := fakeBinary(t, `not json`)

	p := NewApple(bin)
	_, err := p.Embed(context.Background(), []string{"hello"})
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}

func TestAppleProvider_ErrorOnEmptyVector(t *testing.T) {
	bin := fakeBinary(t, `{"result":{"vector":[],"dimension":0}}`)

	p := NewApple(bin)
	_, err := p.Embed(context.Background(), []string{"hello"})
	if err == nil {
		t.Fatal("expected error for empty vector, got nil")
	}
}

// TestAppleProvider_Integration calls the real macos-vision binary.
// Skipped unless macos-vision is found on $PATH.
func TestAppleProvider_Integration(t *testing.T) {
	binPath, err := exec.LookPath("macos-vision")
	if err != nil {
		t.Skip("macos-vision not on $PATH, skipping integration test")
	}

	p := NewApple(binPath)
	vecs, err := p.Embed(context.Background(), []string{"The quick brown fox"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vecs) != 1 || len(vecs[0]) == 0 {
		t.Fatalf("expected one non-empty vector, got %v", vecs)
	}
	t.Logf("apple sentence embedding: dim=%d, vec[0]=%v...", len(vecs[0]), vecs[0][:4])
}
