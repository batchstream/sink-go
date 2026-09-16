package uri

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Key preserves the exact type and bytes used by the built-in Store adapters.
type Key struct {
	Type string
	Data []byte
}

func StringKey(value string) Key {
	key := Key{Type: "string", Data: []byte(value)}
	return key
}

func Int64Key(value int64) Key {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(value))
	key := Key{Type: "int64", Data: data}
	return key
}

func BytesKey(value []byte) Key {
	key := Key{Type: "bytes", Data: bytes.Clone(value)}
	return key
}

func OpaqueKey(kind string, value []byte) Key {
	key := Key{Type: "opaque:" + kind, Data: bytes.Clone(value)}
	return key
}

func FormatKey(key Key) (string, error) {
	switch key.Type {
	case "string":
		if !utf8.Valid(key.Data) {
			return "", errors.New("string key must contain UTF-8; use bytes for binary keys")
		}
		return "s:" + string(key.Data), nil
	case "int64":
		if len(key.Data) != 8 {
			return "", errors.New("int64 key requires eight bytes")
		}
		return "i:" + strconv.FormatInt(int64(binary.BigEndian.Uint64(key.Data)), 10), nil
	case "bytes":
		return "b:" + base64.RawURLEncoding.EncodeToString(key.Data), nil
	default:
		kind, ok := strings.CutPrefix(key.Type, "opaque:")
		if !ok || kind == "" || !utf8.ValidString(kind) {
			return "", errors.New("unsupported key type")
		}
		return "o:" + base64.RawURLEncoding.EncodeToString([]byte(kind)) + ":" + base64.RawURLEncoding.EncodeToString(key.Data), nil
	}
}

func ParseKey(segment string) (Key, error) {
	var empty Key
	tag, value, ok := strings.Cut(segment, ":")
	if !ok {
		return empty, errors.New("record key requires a type tag")
	}
	var key Key
	var err error
	switch tag {
	case "s":
		key.Type = "string"
		key.Data = []byte(value)
	case "b":
		key.Type = "bytes"
		key.Data, err = base64.RawURLEncoding.Strict().DecodeString(value)
	case "i":
		var number int64
		number, err = strconv.ParseInt(value, 10, 64)
		key.Type = "int64"
		key.Data = make([]byte, 8)
		binary.BigEndian.PutUint64(key.Data, uint64(number))
	case "o":
		kind, encoded, found := strings.Cut(value, ":")
		if !found {
			return empty, errors.New("opaque key requires type and data")
		}
		var decoded []byte
		decoded, err = base64.RawURLEncoding.Strict().DecodeString(kind)
		if err != nil {
			return empty, err
		}
		key.Type = "opaque:" + string(decoded)
		key.Data, err = base64.RawURLEncoding.Strict().DecodeString(encoded)
	default:
		return empty, errors.New("unknown record key type tag")
	}
	if err != nil {
		return empty, err
	}
	canonical, err := FormatKey(key)
	if err != nil {
		return empty, err
	}
	if canonical != segment {
		return empty, errors.New("record key is not canonical")
	}
	key.Data = bytes.Clone(key.Data)
	return key, nil
}
