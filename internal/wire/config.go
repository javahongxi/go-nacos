package wire

// Config-domain shapes. JSON tags are byte-aligned with sweet/nacos/config_types.go.
// Note the namespace asymmetry: config calls it "tenant", naming "namespace".

type ConfigQueryRequest struct {
	RequestID string `json:"requestId,omitempty"`
	DataID    string `json:"dataId"`
	Group     string `json:"group"`
	Tenant    string `json:"tenant,omitempty"`
	Tag       string `json:"tag,omitempty"`
}

type ConfigQueryResponse struct {
	Response
	Content          string `json:"content,omitempty"`
	EncryptedDataKey string `json:"encryptedDataKey,omitempty"`
	ContentType      string `json:"contentType,omitempty"`
	MD5              string `json:"md5,omitempty"`
	LastModified     int64  `json:"lastModified"`
	Beta             bool   `json:"beta"`
	Tag              string `json:"tag,omitempty"`
}

// ConfigListenContext is one watched key with the md5 the client holds.
type ConfigListenContext struct {
	Group  string `json:"group"`
	MD5    string `json:"md5"`
	DataID string `json:"dataId"`
	Tenant string `json:"tenant,omitempty"`
}

type ConfigBatchListenRequest struct {
	RequestID            string                `json:"requestId,omitempty"`
	Listen               bool                  `json:"listen"`
	ConfigListenContexts []ConfigListenContext `json:"configListenContexts"`
}

// ConfigContext is the key-only "this one changed" shape.
type ConfigContext struct {
	Group  string `json:"group"`
	DataID string `json:"dataId"`
	Tenant string `json:"tenant,omitempty"`
}

// ConfigChangeBatchListenResponse answers a listen immediately with the stale
// subset — no long-polling suspension in gRPC mode.
type ConfigChangeBatchListenResponse struct {
	Response
	ChangedConfigs []ConfigContext `json:"changedConfigs"`
}

// ConfigChangeNotifyRequest is the server push: key only, never content. The
// client acks, then re-queries and re-listens with the fresh md5.
type ConfigChangeNotifyRequest struct {
	RequestID string `json:"requestId,omitempty"`
	DataID    string `json:"dataId"`
	Group     string `json:"group"`
	Tenant    string `json:"tenant,omitempty"`
}

type ConfigPublishRequest struct {
	RequestID string            `json:"requestId,omitempty"`
	DataID    string            `json:"dataId"`
	Group     string            `json:"group"`
	Tenant    string            `json:"tenant,omitempty"`
	Content   string            `json:"content"`
	CasMD5    string            `json:"casMd5,omitempty"`
	Addition  map[string]string `json:"additionMap,omitempty"`
}

type ConfigPublishResponse struct {
	Response
}

type ConfigRemoveRequest struct {
	RequestID string `json:"requestId,omitempty"`
	DataID    string `json:"dataId"`
	Group     string `json:"group"`
	Tenant    string `json:"tenant,omitempty"`
	Tag       string `json:"tag,omitempty"`
}

type ConfigRemoveResponse struct {
	Response
}
