package k8s

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"

	"gopkg.in/yaml.v3"
)

const (
	inClusterTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	inClusterCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

type Client struct {
	apiserver  string
	token      string
	httpClient *http.Client
	namespace  string
}

func NewClient(namespace, kubeconfigPath string) (*Client, error) {
	if kubeconfigPath != "" {
		return fromKubeconfig(namespace, kubeconfigPath)
	}
	if _, err := os.Stat(inClusterTokenPath); err == nil {
		return fromInCluster(namespace)
	}
	return fromEnv(namespace)
}

func fromInCluster(namespace string) (*Client, error) {
	token, err := os.ReadFile(inClusterTokenPath)
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	ca, err := os.ReadFile(inClusterCAPath)
	if err != nil {
		return nil, fmt.Errorf("read cluster CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("parse cluster CA cert")
	}
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("KUBERNETES_SERVICE_HOST/PORT not set")
	}
	return &Client{
		apiserver: fmt.Sprintf("https://%s:%s", host, port),
		token:     string(token),
		namespace: namespace,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: pool},
			},
		},
	}, nil
}

func fromEnv(namespace string) (*Client, error) {
	apiserver := os.Getenv("KUBE_APISERVER")
	token := os.Getenv("KUBE_TOKEN")
	if apiserver == "" {
		return nil, fmt.Errorf("not in-cluster and KUBE_APISERVER not set")
	}
	return &Client{
		apiserver:  apiserver,
		token:      token,
		namespace:  namespace,
		httpClient: &http.Client{},
	}, nil
}

type kubeconfig struct {
	Clusters []struct {
		Name    string `yaml:"name"`
		Cluster struct {
			Server string `yaml:"server"`
			CAData string `yaml:"certificate-authority-data"`
		} `yaml:"cluster"`
	} `yaml:"clusters"`
	Users []struct {
		Name string `yaml:"name"`
		User struct {
			Token      string `yaml:"token"`
			ClientCert string `yaml:"client-certificate-data"`
			ClientKey  string `yaml:"client-key-data"`
		} `yaml:"user"`
	} `yaml:"users"`
	Contexts []struct {
		Name    string `yaml:"name"`
		Context struct {
			Cluster string `yaml:"cluster"`
			User    string `yaml:"user"`
		} `yaml:"context"`
	} `yaml:"contexts"`
	CurrentContext string `yaml:"current-context"`
}

func fromKubeconfig(namespace, path string) (*Client, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read kubeconfig: %w", err)
	}
	var kc kubeconfig
	if err := yaml.Unmarshal(data, &kc); err != nil {
		return nil, fmt.Errorf("parse kubeconfig: %w", err)
	}
	var clusterName, userName string
	for _, ctx := range kc.Contexts {
		if ctx.Name == kc.CurrentContext {
			clusterName = ctx.Context.Cluster
			userName = ctx.Context.User
			break
		}
	}
	var server string
	for _, cl := range kc.Clusters {
		if cl.Name == clusterName {
			server = cl.Cluster.Server
			break
		}
	}
	var token string
	for _, u := range kc.Users {
		if u.Name == userName {
			token = u.User.Token
			break
		}
	}
	if server == "" {
		return nil, fmt.Errorf("kubeconfig: no server found for context %q", kc.CurrentContext)
	}
	return &Client{
		apiserver:  server,
		token:      token,
		namespace:  namespace,
		httpClient: &http.Client{},
	}, nil
}

func (c *Client) Namespace() string { return c.namespace }

func (c *Client) newRequest(method, path string) (*http.Request, error) {
	url := c.apiserver + path
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	return req, nil
}
