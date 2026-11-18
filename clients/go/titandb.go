package titandb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

// Client speaks TitanDB's length-prefixed TCP wire protocol.
type Client struct {
	conn      net.Conn
	authToken string
}

type wireRequest struct {
	AuthToken string `json:"auth_token,omitempty"`
	Op        string `json:"op"`
	SQL       string `json:"sql,omitempty"`
	Params    []any  `json:"params"`
}

// WireResponse holds query or metrics responses from TitanDB.
type WireResponse struct {
	OK     bool           `json:"ok"`
	Error  string         `json:"error,omitempty"`
	Result map[string]any `json:"result,omitempty"`
}

// Dial connects to a running TitanDB TCP wire server.
func Dial(addr, authToken string, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, authToken: authToken}, nil
}

// Close closes the underlying socket.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Query executes a SQL query over the binary wire protocol.
func (c *Client) Query(sql string, params ...any) (*WireResponse, error) {
	if params == nil {
		params = []any{}
	}
	payload, err := json.Marshal(wireRequest{
		AuthToken: c.authToken,
		Op:        "query",
		SQL:       sql,
		Params:    params,
	})
	if err != nil {
		return nil, err
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := c.conn.Write(hdr[:]); err != nil {
		return nil, err
	}
	if _, err := c.conn.Write(payload); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(c.conn, hdr[:]); err != nil {
		return nil, err
	}
	rlen := binary.BigEndian.Uint32(hdr[:])
	buf := make([]byte, rlen)
	if _, err := io.ReadFull(c.conn, buf); err != nil {
		return nil, err
	}
	var resp WireResponse
	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("titandb error: %s", resp.Error)
	}
	return &resp, nil
}
