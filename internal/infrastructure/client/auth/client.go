package auth

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	authv1 "github.com/dz-market/protobuf/gen/go/auth/api/v1"
)

type Options struct {
	Addr string
}

type Client struct {
	conn *grpc.ClientConn
	rpc  authv1.AuthServiceClient
}

func New(opts Options) (*Client, error) {
	conn, err := grpc.NewClient(opts.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", opts.Addr, err)
	}

	return &Client{
		conn: conn,
		rpc:  authv1.NewAuthServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) GetPublicKey(ctx context.Context) (*rsa.PublicKey, error) {
	resp, err := c.rpc.GetJwks(ctx, &authv1.GetJwksRequest{})
	if err != nil {
		return nil, fmt.Errorf("get jwks: %w", err)
	}

	keys := resp.GetKeys()
	if len(keys) == 0 {
		return nil, errors.New("jwks is empty")
	}

	publicKey, err := parseRSAPublicKey(keys[0].GetN(), keys[0].GetE())
	if err != nil {
		return nil, fmt.Errorf("parse jwks public key: %w", err)
	}

	return publicKey, nil
}

func parseRSAPublicKey(n, e string) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(n)
	if err != nil {
		return nil, fmt.Errorf("decode jwks key n: %w", err)
	}

	eBytes, err := base64.RawURLEncoding.DecodeString(e)
	if err != nil {
		return nil, fmt.Errorf("decode jwks key e: %w", err)
	}

	return &rsa.PublicKey{
		N: new(big.Int).SetBytes(nBytes),
		E: int(new(big.Int).SetBytes(eBytes).Int64()),
	}, nil
}
