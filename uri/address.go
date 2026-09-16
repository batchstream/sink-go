// Package uri defines Sink's canonical, backend-independent record addresses.
// Store adapters interpret path segments; routing compares the complete URI.
package uri

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode/utf8"
)

const MaxAddressBytes = 16 << 10

// Address is immutable. Its canonical URI is its complete record identity.
type Address struct {
	value    string
	store    string
	segments []string
}

func ValidStore(value string) bool {
	if len(value) == 0 || len(value) > 256 {
		return false
	}
	for i, c := range []byte(value) {
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' {
			continue
		}
		if i > 0 && (c == '-' || c == '_' || c == '.') {
			continue
		}
		return false
	}
	return true
}

// Parse accepts only the canonical spelling. It never cleans paths, resolves
// aliases, interprets key types, or applies backend-specific normalization.
func Parse(value string) (Address, error) {
	var empty Address
	if len(value) > MaxAddressBytes {
		return empty, errors.New("sink URI exceeds 16 KiB")
	}
	remainder, ok := strings.CutPrefix(value, "sink://")
	if !ok {
		return empty, errors.New("address must use sink://")
	}
	store, path, found := strings.Cut(remainder, "/")
	if !ValidStore(store) {
		return empty, errors.New("store must use lowercase ASCII letters, digits, dots, underscores or hyphens")
	}
	if !found || path == "" {
		return empty, errors.New("sink URI requires a path")
	}
	encoded := strings.Split(path, "/")
	segments := make([]string, len(encoded))
	for i, part := range encoded {
		decoded, err := url.PathUnescape(part)
		if err != nil {
			return empty, fmt.Errorf("invalid URI escape: %w", err)
		}
		if !validSegment(decoded) {
			return empty, errors.New("URI segments must be nonempty UTF-8 and cannot be dot segments")
		}
		if url.PathEscape(decoded) != part {
			return empty, errors.New("sink URI is not canonical; construct it with the URI builder")
		}
		segments[i] = decoded
	}
	address := Address{value: value, store: store, segments: segments}
	return address, nil
}

func New(store string, segments []string) (Address, error) {
	encoded := make([]string, len(segments))
	for i, segment := range segments {
		encoded[i] = url.PathEscape(segment)
	}
	return Parse("sink://" + store + "/" + strings.Join(encoded, "/"))
}

func validSegment(value string) bool {
	if value == "" || value == "." || value == ".." || !utf8.ValidString(value) {
		return false
	}
	return true
}

func (a Address) String() string     { return a.value }
func (a Address) Store() string      { return a.store }
func (a Address) Segments() []string { return append([]string(nil), a.segments...) }
func (a Address) RoutingKey() string { return a.value }

// AppendKey uses the typed-key convention supported by the built-in stores.
// Custom stores may define other path grammars and construct addresses with New.
func AppendKey(resource string, key Key) (Address, error) {
	var empty Address
	base, err := Parse(resource)
	if err != nil {
		return empty, err
	}
	segment, err := FormatKey(key)
	if err != nil {
		return empty, err
	}
	return Parse(base.String() + "/" + url.PathEscape(segment))
}
