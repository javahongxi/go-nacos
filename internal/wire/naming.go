package wire

// Naming-domain shapes. JSON tags are byte-aligned with sweet/nacos/naming_types.go.
// Jackson serialises Java getters, so derived properties (Service.qualifiedName)
// are mirrored explicitly where they matter (ServiceInfo.Name).

// Instance mirrors api/.../naming/pojo/Instance.java.
type Instance struct {
	InstanceID  string            `json:"instanceId,omitempty"`
	IP          string            `json:"ip"`
	Port        int               `json:"port"`
	Weight      float64           `json:"weight"`
	Healthy     bool              `json:"healthy"`
	Enabled     bool              `json:"enabled"`
	Ephemeral   bool              `json:"ephemeral"`
	ClusterName string            `json:"clusterName,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// ServiceInfo is the payload of query responses and subscriber pushes. Name is
// the qualified name: "GROUP@@service" whenever the group is not DEFAULT_GROUP.
type ServiceInfo struct {
	Name        string     `json:"name,omitempty"`
	GroupName   string     `json:"groupName,omitempty"`
	Clusters    string     `json:"clusters,omitempty"`
	CacheMillis int64      `json:"cacheMillis"`
	Hosts       []Instance `json:"hosts"`
	LastRefTime int64      `json:"lastRefTime"`
	Checksum    string     `json:"checksum,omitempty"`
	AllIPs      bool       `json:"allIps"`
}

type InstanceRequest struct {
	Namespace   string   `json:"namespace,omitempty"`
	ServiceName string   `json:"serviceName"`
	GroupName   string   `json:"groupName,omitempty"`
	Type        string   `json:"type"`
	Instance    Instance `json:"instance"`
}

type InstanceResponse struct {
	Response
	Type string `json:"type"`
}

type ServiceQueryRequest struct {
	Namespace   string `json:"namespace,omitempty"`
	ServiceName string `json:"serviceName"`
	GroupName   string `json:"groupName,omitempty"`
	Cluster     string `json:"cluster,omitempty"`
	HealthyOnly bool   `json:"healthyOnly"`
}

type QueryServiceResponse struct {
	Response
	ServiceInfo ServiceInfo `json:"serviceInfo"`
}

type SubscribeServiceRequest struct {
	Namespace   string `json:"namespace,omitempty"`
	ServiceName string `json:"serviceName"`
	GroupName   string `json:"groupName,omitempty"`
	Clusters    string `json:"clusters,omitempty"`
	Subscribe   bool   `json:"subscribe"`
}

// SubscribeServiceResponse carries the initial snapshot, so a subscriber never
// needs a separate query.
type SubscribeServiceResponse struct {
	Response
	ServiceInfo ServiceInfo `json:"serviceInfo"`
}

// NotifySubscriberRequest is the subscriber push. Measured on a live 3.2.3:
// {"headers":{},"requestId":"5","serviceInfo":{...}} — identity lives in
// serviceInfo.name, there is no top-level serviceName.
type NotifySubscriberRequest struct {
	Headers     map[string]string `json:"headers,omitempty"`
	RequestID   string            `json:"requestId,omitempty"`
	Namespace   string            `json:"namespace,omitempty"`
	ServiceInfo ServiceInfo       `json:"serviceInfo"`
}
