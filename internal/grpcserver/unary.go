package grpcserver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/javahongxi/go-nacos/internal/config"
	"github.com/javahongxi/go-nacos/internal/naming"
	"github.com/javahongxi/go-nacos/internal/wire"
	"github.com/javahongxi/sweet/gen"
)

// handleUnary dispatches on Metadata.type, mirroring GrpcRequestAcceptor:
// ServerCheck is answered before any connection check; everything else needs
// a registered connection, and an unknown type is "RequestHandler Not Found".
func (s *Server) handleUnary(ctx context.Context, p *gen.Payload) (*gen.Payload, error) {
	t := wire.Type(p)
	peerAddr := peerFrom(ctx)

	if t == wire.TypeServerCheckRequest {
		return wire.NewPayload(wire.TypeServerCheckResponse, wire.ServerCheckResponse{
			Response:                  wire.OK(),
			ConnectionID:              fmt.Sprintf("%d_%s", time.Now().UnixMilli(), strings.ReplaceAll(peerAddr, ":", "_")),
			SupportAbilityNegotiation: true,
		})
	}

	c := s.conns.Get(peerAddr)
	if c == nil {
		return wire.NewPayload(wire.TypeErrorResponse, wire.Fail(wire.CodeFail, "Connection is unregistered."))
	}

	switch t {
	case wire.TypeHealthCheckRequest:
		return wire.NewPayload(wire.TypeHealthCheckResponse, wire.HealthCheckResponse{Response: wire.OK()})

	case wire.TypeConfigQueryRequest:
		var req wire.ConfigQueryRequest
		if err := wire.Decode(p, &req); err != nil {
			return badPayload(err)
		}
		it, ok := s.cfg.Query(configKey(req.DataID, req.Group, req.Tenant))
		if !ok {
			return wire.NewPayload(wire.TypeConfigQueryResponse, wire.ConfigQueryResponse{
				Response: wire.Fail(wire.CodeNotFound, "config data not exist"),
			})
		}
		return wire.NewPayload(wire.TypeConfigQueryResponse, wire.ConfigQueryResponse{
			Response:     wire.OK(),
			Content:      it.Content,
			MD5:          it.MD5,
			ContentType:  it.Type,
			LastModified: it.LastModified,
		})

	case wire.TypeConfigPublishRequest:
		var req wire.ConfigPublishRequest
		if err := wire.Decode(p, &req); err != nil {
			return badPayload(err)
		}
		if strings.TrimSpace(req.Content) == "" {
			return wire.NewPayload(wire.TypeConfigPublishResponse, wire.ConfigPublishResponse{
				Response: wire.Fail(wire.CodeInvalidParam, "Required parameter 'content' is blank"),
			})
		}
		res := s.cfg.Publish(configKey(req.DataID, req.Group, req.Tenant), req.Content, req.CasMD5,
			req.Addition["type"], req.Addition["appName"])
		return wire.NewPayload(wire.TypeConfigPublishResponse, wire.ConfigPublishResponse{Response: publishResult(res)})

	case wire.TypeConfigRemoveRequest:
		var req wire.ConfigRemoveRequest
		if err := wire.Decode(p, &req); err != nil {
			return badPayload(err)
		}
		res := s.cfg.Remove(configKey(req.DataID, req.Group, req.Tenant))
		return wire.NewPayload(wire.TypeConfigRemoveResponse, wire.ConfigRemoveResponse{Response: applyResult(res)})

	case wire.TypeConfigBatchListenRequest:
		var req wire.ConfigBatchListenRequest
		if err := wire.Decode(p, &req); err != nil {
			return badPayload(err)
		}
		for i := range req.ConfigListenContexts {
			req.ConfigListenContexts[i].Tenant = tenantOrPublic(req.ConfigListenContexts[i].Tenant)
			if req.ConfigListenContexts[i].Group == "" {
				req.ConfigListenContexts[i].Group = "DEFAULT_GROUP"
			}
		}
		changed := s.cfg.Listen(c.ID, req.ConfigListenContexts, req.Listen)
		return wire.NewPayload(wire.TypeConfigChangeBatchListenResponse, wire.ConfigChangeBatchListenResponse{
			Response:       wire.OK(),
			ChangedConfigs: changed,
		})

	case wire.TypeInstanceRequest:
		var req wire.InstanceRequest
		if err := wire.Decode(p, &req); err != nil {
			return badPayload(err)
		}
		k := namingKey(req.Namespace, req.GroupName, req.ServiceName)
		var resp wire.Response
		switch req.Type {
		case wire.ActionRegisterInstance:
			if err := s.naming.Register(k, req.Instance, c.ID); err != nil {
				resp = wire.Fail(wire.CodeInvalidParam, err.Error())
			} else {
				resp = wire.OK()
			}
		case wire.ActionDeregisterInstance:
			s.naming.Deregister(k, req.Instance, c.ID)
			resp = wire.OK()
		default:
			resp = wire.Fail(wire.CodeInvalidParam, fmt.Sprintf("Unsupported request type %s", req.Type))
		}
		return wire.NewPayload(wire.TypeInstanceResponse, wire.InstanceResponse{Response: resp, Type: req.Type})

	case wire.TypeServiceQueryRequest:
		var req wire.ServiceQueryRequest
		if err := wire.Decode(p, &req); err != nil {
			return badPayload(err)
		}
		info := s.naming.Query(namingKey(req.Namespace, req.GroupName, req.ServiceName), req.Cluster, req.HealthyOnly)
		return wire.NewPayload(wire.TypeQueryServiceResponse, wire.QueryServiceResponse{Response: wire.OK(), ServiceInfo: info})

	case wire.TypeSubscribeServiceRequest:
		var req wire.SubscribeServiceRequest
		if err := wire.Decode(p, &req); err != nil {
			return badPayload(err)
		}
		info := s.naming.Subscribe(namingKey(req.Namespace, req.GroupName, req.ServiceName), c.ID, req.Subscribe)
		return wire.NewPayload(wire.TypeSubscribeServiceResponse, wire.SubscribeServiceResponse{Response: wire.OK(), ServiceInfo: info})

	default:
		s.logger.Warn("no handler for request type", "type", t, "conn", c.ID)
		return wire.NewPayload(wire.TypeErrorResponse, wire.Fail(wire.CodeFail, "RequestHandler Not Found"))
	}
}

func badPayload(err error) (*gen.Payload, error) {
	return wire.NewPayload(wire.TypeErrorResponse, wire.Fail(wire.CodeInvalidParam, err.Error()))
}

func publishResult(res config.ApplyResult) wire.Response {
	if res.Conflict {
		// Nacos routes CAS loss through buildFailResponse(FAIL): resultCode
		// 500, errorCode 500, and the fixed sentence is the only signal.
		return wire.Fail(wire.CodeFail, config.CasConflictMessage)
	}
	return applyResult(res)
}

func applyResult(res config.ApplyResult) wire.Response {
	if !res.OK {
		return wire.Fail(wire.CodeFail, res.Message)
	}
	return wire.OK()
}

func configKey(dataID, group, tenant string) config.Key {
	if group == "" {
		group = "DEFAULT_GROUP"
	}
	return config.Key{DataID: dataID, Group: group, Tenant: tenantOrPublic(tenant)}
}

func namingKey(namespace, group, name string) naming.Key {
	if group == "" {
		group = "DEFAULT_GROUP"
	}
	return naming.Key{Namespace: tenantOrPublic(namespace), Group: group, Name: name}
}

func tenantOrPublic(t string) string {
	if t == "" {
		return "public"
	}
	return t
}
