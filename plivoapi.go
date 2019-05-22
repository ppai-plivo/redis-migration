package main

import (
	"fmt"
	"strings"
)

type smsprefixTransformer struct{}

func (t *smsprefixTransformer) Pattern() string {
	return "smsprefixes"
}

func (t *smsprefixTransformer) Transform(key string) (string, error) {
	return "smsprefixes", nil
}

type numbersTransformer struct{}

func (t *numbersTransformer) Pattern() string {
	return "numbers:*"
}

func (t *numbersTransformer) Transform(key string) (string, error) {
	l := strings.Split(key, ":")
	if len(l) != 2 {
		return "", fmt.Errorf("malformed key %s", key)
	}

	var b strings.Builder
	b.WriteString(l[0]) // numbers
	b.WriteString(":{")
	b.WriteString(l[1]) // auth_id
	b.WriteString("}")

	return b.String(), nil
}

type sandboxTransformer struct{}

func (t *sandboxTransformer) Pattern() string {
	return "sandbox:*"
}

func (t *sandboxTransformer) Transform(key string) (string, error) {
	l := strings.Split(key, ":")
	if len(l) != 2 {
		return "", fmt.Errorf("malformed key %s", key)
	}

	var b strings.Builder
	b.WriteString(l[0]) // sandbox
	b.WriteString(":{")
	b.WriteString(l[1]) // auth_id
	b.WriteString("}")

	return b.String(), nil
}
