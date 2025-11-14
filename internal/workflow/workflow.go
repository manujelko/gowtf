package workflow

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Node struct {
	Name     string `yaml:"name"`
	Type     string `yaml:"type"`
	Upstream string `yaml:"upstream"`
	Script   string `yaml:"script"`
}

type Workflow struct {
	Name     string `yaml:"name"`
	Schedule string `yaml:"schedule"`
	Graph    []Node `yaml:"graph"`
}

func Parse(path string) (*Workflow, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var wf Workflow
	if err = yaml.Unmarshal(data, &wf); err != nil {
		return nil, err
	}

	return &wf, nil
}
