// Package testuri constructs canonical URI fixtures for tests.
package testuri

import "github.com/liran/sink-go/uri"

func Resource(store string, segments []string) string {
	address, err := uri.New(store, segments)
	if err != nil {
		panic(err)
	}
	return address.String()
}
func Record(store string, segments []string, key uri.Key) string {
	address, err := uri.AppendKey(Resource(store, segments), key)
	if err != nil {
		panic(err)
	}
	return address.String()
}
func Address(store string, segments []string, key uri.Key) uri.Address {
	address, err := uri.Parse(Record(store, segments, key))
	if err != nil {
		panic(err)
	}
	return address
}
func Dataset(store, namespace, dataset string, bson bool) string {
	segments := []string{dataset}
	if bson {
		segments = []string{namespace, dataset}
	}
	return Resource(store, segments)
}
