package fakeprovider_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jiahuipaung/model-gateway-lab/internal/fakeprovider"
	"github.com/jiahuipaung/model-gateway-lab/internal/provider"
)

func TestProviderConsumesProgrammedSubmitAndLookupResults(t *testing.T) {
	t.Parallel()

	fake := fakeprovider.New()
	fake.PushSubmit(provider.SubmitResult{ProviderTaskID: "remote-1"}, nil)
	fake.PushLookup(provider.LookupResult{Status: provider.LookupFound, ProviderTaskID: "remote-1"}, nil)

	submitted, err := fake.Submit(context.Background(), provider.SubmitRequest{RequestKey: "request-1"})
	if err != nil || submitted.ProviderTaskID != "remote-1" {
		t.Fatalf("Submit = %#v, %v", submitted, err)
	}
	lookup, err := fake.Lookup(context.Background(), "request-1")
	if err != nil || lookup.Status != provider.LookupFound {
		t.Fatalf("Lookup = %#v, %v", lookup, err)
	}
	if _, err := fake.Submit(context.Background(), provider.SubmitRequest{}); !errors.Is(err, fakeprovider.ErrNoBehavior) {
		t.Fatalf("empty script error = %v, want ErrNoBehavior", err)
	}
}
