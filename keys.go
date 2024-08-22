package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type CompletedManifest struct {
	Keys []string `json:"keys"`
}

func ExtractKeysFromCompletedManifest(completedManifestLocalPath string) ([]string, error) {
	cp := &CompletedManifest{Keys: make([]string, 0)}
	b, err := os.ReadFile(completedManifestLocalPath)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, cp)
	if err != nil {
		return nil, err
	}
	if len(cp.Keys) == 0 {
		return nil, fmt.Errorf("no keys found in %s", completedManifestLocalPath)
	}
	return cp.Keys, nil
}
