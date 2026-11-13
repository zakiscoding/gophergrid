package jobs

import "testing"

func TestNormalizeDefaults(t *testing.T) {
	s := Spec{Type: TypeSleep}
	if err := s.Normalize(); err != nil {
		t.Fatal(err)
	}
	if s.TimeoutMS != DefaultTimeoutMS || s.MaxAttempts != DefaultMaxAttempts ||
		s.Resources.CPUMillis != 100 || s.Resources.MemoryMB != 64 || string(s.Payload) != "{}" {
		t.Fatalf("defaults not applied: %+v", s)
	}
}

func TestNormalizeRejects(t *testing.T) {
	bad := []Spec{
		{},
		{Type: "rm -rf"},
		{Type: TypeSleep, Payload: []byte(`{`)},
		{Type: TypeSleep, Priority: 101},
		{Type: TypeSleep, TimeoutMS: -1},
		{Type: TypeSleep, MaxAttempts: 21},
		{Type: TypeSleep, Resources: Resources{CPUMillis: -5}},
		{Type: TypeSleep, Resources: Resources{GPUMemoryMB: 100}},
		{Type: TypeInference},
	}
	for _, s := range bad {
		if err := s.Normalize(); err == nil {
			t.Errorf("accepted %+v", s)
		}
	}
}
