package provider

import (
	"errors"
	"testing"
)

func TestSubmissionErrorsCarryMachineReadableSemantics(t *testing.T) {
	t.Parallel()

	definite := &DefiniteError{Code: "invalid_input"}
	unknown := &UnknownOutcomeError{Cause: errors.New("response lost")}

	var gotDefinite *DefiniteError
	if !errors.As(definite, &gotDefinite) || gotDefinite.Code != "invalid_input" {
		t.Fatalf("expected typed definite error, got %#v", definite)
	}

	var gotUnknown *UnknownOutcomeError
	if !errors.As(unknown, &gotUnknown) || !errors.Is(unknown, unknown.Cause) {
		t.Fatalf("expected typed unknown outcome with unwrap, got %#v", unknown)
	}
}

func TestLookupAndPollStatusesAreDistinct(t *testing.T) {
	t.Parallel()

	if LookupFound == LookupAbsent || LookupAbsent == LookupInconclusive {
		t.Fatal("lookup outcomes must be distinct")
	}
	if PollQueued == PollRunning || PollRunning == PollSucceeded || PollSucceeded == PollFailed {
		t.Fatal("poll outcomes must be distinct")
	}
}
