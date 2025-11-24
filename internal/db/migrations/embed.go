package migrations

import (
	_ "embed"
)

//go:embed 0001_initial.sql
var InitialMigration string
