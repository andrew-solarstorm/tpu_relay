package main

import "errors"

type (
	RPCParams  []any
	RPCRequest struct {
		Jsonrpc string    `json:"jsonrpc"`
		ID      any       `json:"id"`
		Method  string    `json:"method"`
		Params  RPCParams `json:"params"`
	}
)

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}
type RPCResponse struct {
	Jsonrpc string    `json:"jsonrpc"`
	ID      any       `json:"id"`
	Result  string    `json:"result,omitempty"`
	Error   *RPCError `json:"error,omitempty"`
}

var (
	ErrMissingParams       = errors.New("missing params")
	ErrInvalidParam        = errors.New("invalid param")
	ErrInvalidKey          = errors.New("invalid key")
	ErrInvalidOptions      = errors.New("invalid options")
	ErrNotFound            = errors.New("not found")
	ErrMaxAccounts         = errors.New("max 255 accounts per call")
	ErrInvalidFilters      = errors.New("invalid filters")
	ErrInvalidCursorFilter = errors.New("invalid cursor filter")
	ErrInvalidDataSlice    = errors.New("invalid data slice")
	ErrInvalidEncoding     = errors.New("invalid encoding")
	ErrUnsuportedMethod    = errors.New("method not supported")
	ErrInternal            = errors.New("internal error")
	ErrInvalidRPCVersion   = errors.New("invalid RPC version")
)

// RPC Error Codes
const (
	ErrCodeInvalidRequest       = -32600
	ErrCodeInvalidParams        = -32602
	ErrCodeParseError           = -32700
	ErrCodeInvalidRequestError  = -32600
	ErrCodeMethodNotFoundError  = -32601
	ErrCodeInvalidParamsError   = -32602
	ErrCodeInternalError        = -32603
	ErrCodeServerError          = -32000
	ErrCodeErrCodeInternalError = -32603
)
