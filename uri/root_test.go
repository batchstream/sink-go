package uri

import "testing"

func TestRootResourceURIIsCanonical(t *testing.T) {
	address, err := New("search", nil)
	if err != nil || address.String() != "sink://search" || address.Store() != "search" || len(address.Segments()) != 0 || address.EscapedPath() != "" {
		t.Fatalf("invalid Store resource: %+v %v", address, err)
	}
	for _, value := range []string{"sink://search/", "sink://search?x=y", "sink://search#x", "sink://search/products/"} {
		if _, err := Parse(value); err == nil {
			t.Errorf("accepted noncanonical resource %q", value)
		}
	}
}
