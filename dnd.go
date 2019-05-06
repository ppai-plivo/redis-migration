package main

import (
	"fmt"
	"strings"
)

type dndTransformer struct{}

func (t *dndTransformer) Pattern() string {
	return "stop*"
}

func (t *dndTransformer) Transform(key string) (string, error) {
	l := strings.Split(key, ":")
	if len(l) != 3 {
		return "", fmt.Errorf("malformed key %s", key)
	}

	var b strings.Builder
	b.WriteString(l[0]) // stop
	b.WriteString(":")
	b.WriteString(l[1]) // src number or powerpack
	b.WriteString(":{")
	b.WriteString(l[2]) // destination number
	b.WriteString("}")

	return b.String(), nil
}
