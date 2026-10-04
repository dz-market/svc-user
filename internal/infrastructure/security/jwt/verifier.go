package jwt

import (
	"crypto/rsa"
	"fmt"
	"uuid"

	jwtgo "github.com/golang-jwt/jwt/v5"
)

type Claims struct {
	jwtgo.RegisteredClaims
}

type Verifier struct {
	parser *jwtgo.Parser
	key    *rsa.PublicKey
}

func NewVerifier(key *rsa.PublicKey) *Verifier {
	return &Verifier{
		parser: jwtgo.NewParser(
			jwtgo.WithValidMethods([]string{jwtgo.SigningMethodRS256.Alg()}),
			jwtgo.WithExpirationRequired(),
		),
		key: key,
	}
}

func (v *Verifier) Verify(token string) (uuid.UUID, error) {
	var claims Claims

	if _, err := v.parser.ParseWithClaims(
		token, &claims, func(*jwtgo.Token) (any, error) {
			return v.key, nil
		},
	); err != nil {
		return uuid.Nil(), fmt.Errorf("invalid token: %w", err)
	}

	userID, err := uuid.Parse(claims.Subject)
	if err != nil {
		return uuid.Nil(), fmt.Errorf("parse subject: %w", err)
	}

	return userID, nil
}
