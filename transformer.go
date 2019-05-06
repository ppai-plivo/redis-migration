package main

type KeyTransformer interface {
	Pattern() string
	Transform(key string) (string, error)
}
