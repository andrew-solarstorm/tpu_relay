package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/andrew-solarstorm/digo"
	"github.com/bytedance/sonic"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/mr-tron/base58"
	"github.com/rs/zerolog/log"
)

const HTTP_SVC = "http-service"

type HTTPService struct {
	digo.BaseDIInstance

	port   int
	host   string
	rpcUrl string

	server   *http.Server
	relaySvc *RelayService

	httpClient *http.Client
}

func (svc *HTTPService) ID() string {
	return HTTP_SVC
}

func (svc *HTTPService) Configure(c digo.IContainer) error {
	svc.relaySvc = digo.Instance[*RelayService](RELAY_SVC, c)
	conf := digo.GetConfig[*Config](c)
	svc.host = conf.HTTPHost
	svc.port = conf.HTTPPort
	svc.rpcUrl = conf.RPCUrl

	svc.httpClient = http.DefaultClient
	return nil
}

func (svc *HTTPService) Start() error {
	r := gin.New()

	config := cors.DefaultConfig()
	config.AllowAllOrigins = true
	config.AllowCredentials = true
	config.AddAllowHeaders("Authorization")
	r.Use(cors.New(config))
	r.POST("/", svc.handler)

	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", svc.host, svc.port),
		Handler: r,
	}
	svc.server = server
	return server.ListenAndServe()
}

func (svc *HTTPService) Stop() error {
	return svc.server.Close()
}

func (svc *HTTPService) handleErr(c *gin.Context, err error, code int) {
	res := RPCResponse{Jsonrpc: "2.0", Error: &RPCError{
		Code:    code,
		Message: err.Error(),
	}}

	buf, err := sonic.Marshal(res)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to marshal response"})
		return
	}
	c.Data(http.StatusBadRequest, "application/json", buf)
}

func (svc *HTTPService) handler(c *gin.Context) {
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		svc.handleErr(c, err, ErrCodeInvalidParams)
		return
	}

	// Single request
	var request RPCRequest
	if err := json.Unmarshal(bodyBytes, &request); err != nil {
		svc.handleErr(c, err, ErrCodeInvalidParams)
		return
	}

	if request.Method == "sendTransaction" {
		svc.sendTxn(&request, c)
	} else {
		req, err := http.NewRequest("POST", svc.rpcUrl, bytes.NewReader(bodyBytes))
		if err != nil {
			c.AbortWithStatus(http.StatusBadGateway)
			return
		}
		// Copy headers from the original request
		for key, values := range c.Request.Header {
			for _, v := range values {
				req.Header.Add(key, v)
			}
		}
		resp, err := svc.httpClient.Do(req)
		if err != nil {
			c.AbortWithStatus(http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		// Copy response headers back
		for key, values := range resp.Header {
			for _, v := range values {
				c.Header(key, v)
			}
		}

		// Write status and body
		c.Status(resp.StatusCode)
		io.Copy(c.Writer, resp.Body)
	}
}

func (svc *HTTPService) sendTxn(req *RPCRequest, c *gin.Context) {
	params := req.Params
	encData, ok := params[0].(string)
	if !ok {
		svc.handleErr(c, ErrInvalidParam, ErrCodeInvalidParams)
		return
	}

	opts := &rpc.TransactionOpts{
		Encoding: "base64",
	}

	// in case options come
	if len(params) > 1 {
		m, ok := params[1].(map[string]any)
		if !ok {
			svc.handleErr(c, ErrInvalidParam, ErrCodeInvalidParams)
			return
		}

		if val, ok := m["encoding"].(string); ok {
			opts.Encoding = solana.EncodingType(val)
		}
		if val, ok := m["preflightCommitment"].(string); ok {
			opts.PreflightCommitment = rpc.CommitmentType(val)
		}
		if val, ok := m["skipPreflight"].(bool); ok {
			opts.SkipPreflight = val
		}
	}

	var txReqBytes []byte
	var err error
	switch opts.Encoding {
	case "base64":
		txReqBytes, err = base64.StdEncoding.DecodeString(encData)
	case "base58":
		txReqBytes, err = base58.Decode(encData)
	default:
		err = ErrInvalidEncoding
	}
	if err != nil {
		svc.handleErr(c, err, ErrCodeInvalidParams)
		return
	}
	tx, err := solana.TransactionFromBytes(txReqBytes)
	if err != nil {
		svc.handleErr(c, ErrInternal, ErrCodeInternalError)
		return
	}

	// send to 0 slot ahead and 2 leaders from the target slot.
	err = svc.relaySvc.Relay().SendRawTransaction(c.Request.Context(), txReqBytes, 0, 2)
	if err != nil {
		svc.handleErr(c, err, ErrCodeInternalError)
		return
	}

	sig := tx.Signatures[0]
	log.Info().Str("signature", sig.String()).Int("size", len(txReqBytes)).Uint64("at", uint64(time.Now().UnixMilli())).Msg("tx sent")

	resp := &RPCResponse{
		ID:      req.ID,
		Jsonrpc: req.Jsonrpc,
		Result:  sig.String(),
	}
	data, err := sonic.Marshal(resp)
	if err != nil {
		svc.handleErr(c, err, ErrCodeInternalError)
		return
	}
	c.Data(http.StatusOK, "application/json", data)
}
