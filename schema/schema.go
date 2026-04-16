// Package schema holds embedded SQLite DDL strings.
package schema

import _ "embed"

//go:embed source.sql
var SourceDDL string

//go:embed packages.sql
var PackagesDDL string

//go:embed symbols.sql
var SymbolsDDL string
