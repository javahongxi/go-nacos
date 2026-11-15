// Package wire is the server-side view of the Nacos gRPC envelope: the Java
// class simple name travels in Metadata.type, the body is the object's JSON
// wrapped in an Any with an empty type_url. Shapes mirror sweet/nacos (which
// were verified against a live Nacos 3.2.3), so the two repos stay wire-twins.
package wire

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/javahongxi/sweet/gen"
)

// Metadata.type values (Java class simple names).
const (
	TypeServerCheckRequest     = "ServerCheckRequest"
	TypeServerCheckResponse    = "ServerCheckResponse"
	TypeConnectionSetupRequest = "ConnectionSetupRequest"
	TypeSetupAckRequest        = "SetupAckRequest"
	TypeSetupAckResponse       = "SetupAckResponse"
	TypeHealthCheckRequest     = "HealthCheckRequest"
	TypeHealthCheckResponse    = "HealthCheckResponse"
	TypeErrorResponse          = "ErrorResponse"

	TypeConfigQueryRequest              = "ConfigQueryRequest"
	TypeConfigQueryResponse             = "ConfigQueryResponse"
	TypeConfigPublishRequest            = "ConfigPublishRequest"
	TypeConfigPublishResponse           = "ConfigPublishResponse"
	TypeConfigRemoveRequest             = "ConfigRemoveRequest"
	TypeConfigRemoveResponse            = "ConfigRemoveResponse"
	TypeConfigBatchListenRequest        = "ConfigBatchListenRequest"
	TypeConfigChangeBatchListenResponse = "ConfigChangeBatchListenResponse"
	TypeConfigChangeNotifyRequest       = "ConfigChangeNotifyRequest"

	TypeInstanceRequest          = "InstanceRequest"
	TypeInstanceResponse         = "InstanceResponse"
	TypeServiceQueryRequest      = "ServiceQueryRequest"
	TypeQueryServiceResponse     = "QueryServiceResponse"
	TypeSubscribeServiceRequest  = "SubscribeServiceRequest"
	TypeSubscribeServiceResponse = "SubscribeServiceResponse"
	TypeNotifySubscriberRequest  = "NotifySubscriberRequest"

	// InstanceRequest.type values (NamingRemoteConstants), not Metadata.type.
	ActionRegisterInstance   = "registerInstance"
	ActionDeregisterInstance = "deregisterInstance"
)

// Response codes: success is resultCode==200; setErrorInfo puts resultCode at
// 500 and moves the specific code into errorCode (Response.java:118-122).
const (
	CodeSuccess      = 200
	CodeNotFound     = 300
	CodeInvalidParam = 400
	CodeNoRight      = 403
	CodeFail         = 500
)

// Response is the shared base of every Nacos response. Success is Jackson's
// derived isSuccess() property and must be serialised — clients check it.
type Response struct {
	ResultCode int    `json:"resultCode"`
	ErrorCode  int    `json:"errorCode"`
	Message    string `json:"message,omitempty"`
	RequestID  string `json:"requestId,omitempty"`
	Success    bool   `json:"success"`
}

func OK() Response {
	return Response{ResultCode: CodeSuccess, Success: true}
}

// Fail mirrors setErrorInfo: resultCode 500, specific code in errorCode.
func Fail(code int, msg string) Response {
	return Response{ResultCode: CodeFail, ErrorCode: code, Message: msg}
}

// NewPayload renders a response/push frame: type only in metadata (Nacos'
// convert(Response) leaves clientIp empty), JSON bytes in Any.value with an
// empty type_url — the peer reads value, never resolves the type.
func NewPayload(typeName string, body any) (*gen.Payload, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("wire: marshal %s: %w", typeName, err)
	}
	return &gen.Payload{
		Metadata: &gen.Metadata{Type: typeName},
		Body:     &anypb.Any{Value: b},
	}, nil
}

// Type reports the frame's claimed class name.
func Type(p *gen.Payload) string { return p.GetMetadata().GetType() }

// Decode unpacks a frame's JSON body into out.
func Decode(p *gen.Payload, out any) error {
	if err := json.Unmarshal(p.GetBody().GetValue(), out); err != nil {
		return fmt.Errorf("wire: decode %s: %w", Type(p), err)
	}
	return nil
}

// ConnectionSetupRequest is the bidi handshake opener. AbilityTable must be
// non-null or the server stays silent (GrpcBiStreamRequestAcceptor:203-212).
type ConnectionSetupRequest struct {
	ClientVersion string            `json:"clientVersion"`
	Tenant        string            `json:"tenant,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
	AbilityTable  map[string]bool   `json:"abilityTable"`
}

// SetupAckRequest is pushed back over the same stream to acknowledge setup.
type SetupAckRequest struct {
	AbilityTable map[string]bool `json:"abilityTable,omitempty"`
}

// ServerCheckResponse answers the unary probe; connectionId is informational
// for go-nacos (identity is the peer addr), but the field must exist.
type ServerCheckResponse struct {
	Response
	ConnectionID              string `json:"connectionId,omitempty"`
	SupportAbilityNegotiation bool   `json:"supportAbilityNegotiation"`
}

// HealthCheckResponse answers the idle keepalive probe.
type HealthCheckResponse struct {
	Response
}
