package workflow

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Workflow struct {
	Name     string            `yaml:"name"`
	Schedule string            `yaml:"schedule"`
	Env      map[string]string `yaml:"env,omitempty"`
	Tasks    []Task            `yaml:"tasks"`
}

type Task struct {
	Name       string            `yaml:"name"`
	DependsOn  []string          `yaml:"depends_on,omitempty"`
	Condition  string            `yaml:"condition,omitempty"`
	Script     string            `yaml:"script"`
	Env        map[string]string `yaml:"env,omitempty"`
	Retries    int               `yaml:"retries,omitempty"`
	RetryDelay string            `yaml:"retry_delay,omitempty"`
	Timeout    string            `yaml:"timeout,omitempty"`
}

func Load(path string) (*Workflow, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var wf Workflow
	if err = yaml.Unmarshal(data, &wf); err != nil {
		return nil, fmt.Errorf("yaml error: %w", err)
	}

	if err = validateWorkflow(&wf); err != nil {
		return nil, fmt.Errorf("validation error: %w", err)
	}

	return &wf, nil
}
