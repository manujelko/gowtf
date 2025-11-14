package workflow

import "testing"

func TestParseHello(t *testing.T) {
	wf, err := Parse("testdata/hello.yaml")
	if err != nil {
		t.Fatalf("failed to read fixture: %v", err)
	}

	expectedName := "hello"
	if wf.Name != expectedName {
		t.Errorf("incorrect workflow name, got %s, want %s", wf.Name, expectedName)
	}

	expectedSchedule := "* * * * *"
	if wf.Schedule != expectedSchedule {
		t.Errorf("incorrect workflow schedule, got %s, want %s", wf.Schedule, expectedSchedule)
	}

	expectedNodes := 1
	if len(wf.Graph) != expectedNodes {
		t.Errorf("incorrect number of nodes, got %d, want %d", len(wf.Graph), expectedNodes)
	}

	node := wf.Graph[0]

	expectedNodeName := "hello"
	if node.Name != expectedNodeName {
		t.Errorf("incorrect node name, got %s, want %s", node.Name, expectedNodeName)
	}

	expectedNodeType := "task"
	if node.Type != expectedNodeType {
		t.Errorf("incorrect node type, got %s, want %s", node.Type, expectedNodeType)
	}

	expectedScript := "echo \"Hello, World\""
	if node.Script != expectedScript {
		t.Errorf("incorrect script, got %s, want %s", node.Script, expectedScript)
	}
}
