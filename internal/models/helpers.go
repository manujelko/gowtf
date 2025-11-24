package models

import (
	"database/sql"
	"encoding/json"
)

func encodeEnv(env map[string]string) (sql.NullString, error) {
	if env == nil {
		return sql.NullString{}, nil
	}

	b, err := json.Marshal(env)
	if err != nil {
		return sql.NullString{}, err
	}

	return sql.NullString{
		String: string(b),
		Valid:  true,
	}, nil
}

func decodeEnv(ns sql.NullString) (map[string]string, error) {
	if !ns.Valid || ns.String == "" {
		return map[string]string{}, nil
	}

	var m map[string]string
	err := json.Unmarshal([]byte(ns.String), &m)
	return m, err
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
