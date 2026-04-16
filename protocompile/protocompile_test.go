package protocompile

// TestCompileSelf was removed when this package moved out of proto-repo.
// It walked the enclosing module root for .proto files, which only worked
// while protocompile lived inside a module that happened to ship protos.
// cmd/indexer/e2e_test.go exercises Compile end-to-end against a synthetic
// repo, so the self-test is redundant.
