package auth

import "errors"

var (
	// ErrMissingToken is returned when no bearer credential is present.
	ErrMissingToken = errors.New("auth: missing bearer token")
	// ErrMalformedToken is returned when the token cannot be parsed.
	ErrMalformedToken = errors.New("auth: malformed token")
	// ErrInvalidSignature is returned when JWT signature verification fails.
	ErrInvalidSignature = errors.New("auth: invalid signature")
	// ErrExpired is returned when the token is past its expiry.
	ErrExpired = errors.New("auth: token expired")
	// ErrWrongIssuer is returned when the iss claim does not match configuration.
	ErrWrongIssuer = errors.New("auth: wrong issuer")
	// ErrWrongAudience is returned when no configured audience matches aud.
	ErrWrongAudience = errors.New("auth: wrong audience")
	// ErrMissingSubject is returned when sub is empty after verification.
	ErrMissingSubject = errors.New("auth: missing subject")
)
