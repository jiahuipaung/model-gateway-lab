package task

import "testing"

func TestSubmissionStateTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from SubmissionState
		to   SubmissionState
		want bool
	}{
		{"pending can be claimed", SubmissionPending, SubmissionSubmitting, true},
		{"submitting can be confirmed", SubmissionSubmitting, SubmissionConfirmed, true},
		{"submitting can be rejected", SubmissionSubmitting, SubmissionRejected, true},
		{"submitting can become unknown", SubmissionSubmitting, SubmissionUnknown, true},
		{"unknown can be confirmed", SubmissionUnknown, SubmissionConfirmed, true},
		{"unknown can be safely requeued", SubmissionUnknown, SubmissionPending, true},
		{"confirmed cannot be resubmitted", SubmissionConfirmed, SubmissionSubmitting, false},
		{"rejected is terminal", SubmissionRejected, SubmissionPending, false},
		{"pending cannot skip submitting", SubmissionPending, SubmissionConfirmed, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.from.CanTransitionTo(tt.to); got != tt.want {
				t.Fatalf("CanTransitionTo(%q) = %v, want %v", tt.to, got, tt.want)
			}
		})
	}
}

func TestExecutionStateTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from ExecutionState
		to   ExecutionState
		want bool
	}{
		{"none starts queued", ExecutionNone, ExecutionQueued, true},
		{"queued stays observable", ExecutionQueued, ExecutionQueued, true},
		{"queued can run", ExecutionQueued, ExecutionRunning, true},
		{"queued can complete", ExecutionQueued, ExecutionSucceeded, true},
		{"running can complete", ExecutionRunning, ExecutionSucceeded, true},
		{"running can fail", ExecutionRunning, ExecutionFailed, true},
		{"success is terminal", ExecutionSucceeded, ExecutionRunning, false},
		{"failure is terminal", ExecutionFailed, ExecutionQueued, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.from.CanTransitionTo(tt.to); got != tt.want {
				t.Fatalf("CanTransitionTo(%q) = %v, want %v", tt.to, got, tt.want)
			}
		})
	}
}

func TestCanonicalDigestIsStableAndSensitiveToMeaningfulFields(t *testing.T) {
	t.Parallel()

	base := Spec{
		Provider:  "fake-a",
		Model:     "video-basic",
		Operation: "text-to-video",
		Prompt:    "a paper boat",
	}

	first := CanonicalDigest(base)
	second := CanonicalDigest(base)
	if first == "" || first != second {
		t.Fatalf("digest must be non-empty and stable: %q != %q", first, second)
	}

	changed := base
	changed.Prompt = "a red paper boat"
	if CanonicalDigest(changed) == first {
		t.Fatal("changing the prompt must change the digest")
	}
}
