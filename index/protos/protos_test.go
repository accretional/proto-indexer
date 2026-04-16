package protos

// TestIndexSelf was removed when this package moved out of proto-repo.
// It compiled the enclosing module's own .proto files and indexed the
// resulting FDS, which only worked while this package lived inside a
// module that shipped protos. cmd/indexer/e2e_test.go exercises Index
// end-to-end against a synthetic repo, so the self-test is redundant.
