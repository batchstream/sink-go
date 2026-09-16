package uri

import (
	"bytes"
	"testing"
)

func TestURIIsCanonicalAndOpaque(t *testing.T) {
	segments := []string{"数据库", "collection", "s:a/b%?#\x00"}
	address, err := New("catalog", segments)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(address.String())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Store() != "catalog" || parsed.Segments()[2] != segments[2] {
		t.Fatal(parsed)
	}
	copy := parsed.Segments()
	copy[0] = "changed"
	if parsed.Segments()[0] != segments[0] {
		t.Fatal("mutable address")
	}
	for _, value := range []string{"http://catalog/a", "sink://Catalog/a", "sink://catalog", "sink://catalog/a/", "sink://catalog/a//b", "sink://catalog/../b", "sink://catalog/a?query", "sink://catalog/a#fragment", "sink://catalog/%61", "sink://catalog/a%2fb", "sink://user@catalog/a", "sink://catalog:8080/a", "sink://catalog/%ff"} {
		if _, err := Parse(value); err == nil {
			t.Errorf("accepted noncanonical URI %q", value)
		}
	}
}

func TestTypedKeyRoundTripAndIdentity(t *testing.T) {
	keys := []Key{StringKey("123"), Int64Key(123), BytesKey([]byte("123")), OpaqueKey("mongodb/object-id", []byte("123")), StringKey(""), StringKey("a/b?c#d%\x00"), Int64Key(-9223372036854775808), BytesKey([]byte{0, 255})}
	seen := make(map[string]bool)
	for _, key := range keys {
		address, err := AppendKey("sink://catalog/db/items", key)
		if err != nil {
			t.Fatal(err)
		}
		if seen[address.String()] {
			t.Fatal("key types collided")
		}
		seen[address.String()] = true
		segments := address.Segments()
		parsed, err := ParseKey(segments[len(segments)-1])
		if err != nil || parsed.Type != key.Type || !bytes.Equal(parsed.Data, key.Data) {
			t.Fatalf("round trip %v: %v %v", key, parsed, err)
		}
	}
	for _, segment := range []string{"i:01", "i:+1", "i:-0", "i:9223372036854775808", "b:YQ==", "b:YR", "o::YQ", "x:123", "123"} {
		if _, err := ParseKey(segment); err == nil {
			t.Errorf("accepted noncanonical key %q", segment)
		}
	}
}

func FuzzCanonicalURI(f *testing.F) {
	f.Add("catalog", "db", "a/b?c#d%")
	f.Fuzz(func(t *testing.T, store, resource, value string) {
		key := StringKey(value)
		base, err := New(store, []string{resource})
		if err != nil {
			return
		}
		address, err := AppendKey(base.String(), key)
		if err != nil {
			return
		}
		parsed, err := Parse(address.String())
		if err != nil || parsed.String() != address.String() {
			t.Fatal("unstable canonical URI")
		}
		parts := parsed.Segments()
		decoded, err := ParseKey(parts[len(parts)-1])
		if err != nil || decoded.Type != key.Type || !bytes.Equal(decoded.Data, key.Data) {
			t.Fatal("key did not round trip")
		}
	})
}
