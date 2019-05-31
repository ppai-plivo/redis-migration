package main

import (
	"fmt"
	"strings"
)

type senderidTransformer struct{}

func (t *senderidTransformer) Pattern() string {
	return "senderid:*"
}

func (t *senderidTransformer) Transform(key string) (string, error) {
	// senderid:<country_id>:<auth_id>:<carrier_id>
	// senderid:<country_id>:default:<carrier_id>

	l := strings.Split(key, ":")
	if len(l) != 4 {
		return "", fmt.Errorf("malformed senderid key %s", key)
	}

	var b strings.Builder
	b.WriteString(l[0]) // senderid
	b.WriteString(":")
	b.WriteString(l[1]) // country_id
	b.WriteString(":")
	b.WriteString("{")
	b.WriteString(l[2]) // auth_id/default
	b.WriteString("}")
	b.WriteString(":")
	b.WriteString(l[3]) // carrier_id

	return b.String(), nil
}

type ratelimitTransformer struct{}

func (t *ratelimitTransformer) Pattern() string {
	return "sms_rate_limit:*"
}

func (t *ratelimitTransformer) Transform(key string) (string, error) {
	return key, nil
}
