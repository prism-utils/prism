package authtest

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	jwt "github.com/go-jose/go-jose/v4/jwt"
)

// UnsafeAlgNoneToken builds an unsigned JWT with alg=none for negative testing.
func UnsafeAlgNoneToken(issuer, audience, subject string) (string, error) {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	claims, err := json.Marshal(map[string]any{
		"sub": subject,
		"aud": audience,
		"iss": issuer,
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})
	if err != nil {
		return "", err
	}
	payload := base64.RawURLEncoding.EncodeToString(claims)
	return header + "." + payload + ".", nil
}

// HMACConfusionToken signs a token with HS256 using the RSA public key bytes.
func HMACConfusionToken(env *JWTEnv) (string, error) {
	pub := env.privateKey.PublicKey
	nBytes := pub.N.Bytes()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims, err := json.Marshal(map[string]any{
		"sub": "attacker",
		"aud": env.Audience,
		"iss": env.Issuer,
		"exp": jwt.NewNumericDate(time.Now().Add(time.Hour)),
		"iat": jwt.NewNumericDate(time.Now()),
	})
	if err != nil {
		return "", err
	}
	payload := base64.RawURLEncoding.EncodeToString(claims)
	signingInput := header + "." + payload
	mac := hmac.New(sha256.New, nBytes)
	_, _ = mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sig, nil
}

// TamperedPayloadSuffix replaces the payload segment while keeping the signature.
func TamperedPayloadSuffix(token string, payloadJSON string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("authtest: not a compact JWT")
	}
	payload := base64.RawURLEncoding.EncodeToString([]byte(payloadJSON))
	return parts[0] + "." + payload + "." + parts[2], nil
}
