package agollo

import (
	"os"
	"time"
)





var (
	defaultCluster                    = "default"
	defaultNamespace                  = "application"
	defaultBackupFile                 = ".agollo"
	defaultAutoFetchOnCacheMiss       = false
	defaultFailTolerantOnBackupExists = false
	defaultEnableSLB                  = false
	defaultLongPollInterval           = 1 * time.Second
)


type Options struct {

	AppID                      string        // appid
	Cluster                    string        // 默认的集群名称，默认：default
	DefaultNamespace           string        // 默认的命名空间，默认：application
	PreloadNamespaces          []string      // 预加载命名空间，默认：application

	ApolloClient               ApolloClient  // apollo HTTP api实现

	Logger                     Logger        // 需要日志需要设置实现，或者注入有效的io.Writer，默认: ioutil.Discard
	AutoFetchOnCacheMiss       bool          // 自动获取非预设以外的namespace的配置，默认：false
	LongPollerInterval         time.Duration // 轮训间隔时间，默认：1s
	BackupFile                 string        // 备份文件存放地址，默认：.agollo
	FailTolerantOnBackupExists bool          // 服务器连接失败时允许读取备份，默认：false
	Balancer                   Balancer      // ConfigServer负载均衡
	EnableSLB                  bool          // 启用ConfigServer负载均衡
	RefreshIntervalInSecond    time.Duration // ConfigServer刷新间隔
}



// 初始化默认配置
// 初始化特性配置
// 初始化负载均衡器
// 设置预加载 namespace

func newOptions(configServerURL, appID string, opts ...Option) (Options, error) {

	// 默认配置
	var options = Options{
		AppID:                      appID,
		Cluster:                    defaultCluster,
		DefaultNamespace:           defaultNamespace,
		ApolloClient:               NewApolloClient(),
		Logger:                     NewLogger(),
		AutoFetchOnCacheMiss:       defaultAutoFetchOnCacheMiss,
		LongPollerInterval:         defaultLongPollInterval,
		BackupFile:                 defaultBackupFile,
		FailTolerantOnBackupExists: defaultFailTolerantOnBackupExists,
		EnableSLB:                  defaultEnableSLB,
	}

	// 设置
	for _, opt := range opts {
		opt(&options)
	}

	// 负载均衡器
	if options.Balancer == nil {
		var b Balancer
		// 从配置或者环境变量中获取 ConfigServerAddrs
		configServerURLs := getConfigServers(configServerURL)
		// 如果客户端开启了 EnableSLB 或者没有指定 ConfigServerAddrs 地址，就需要定时、自动的通过 MetaServer 获取
		// 最新的 ConfigServerAddrs 来更新本地负载均衡器。
		if options.EnableSLB || len(configServerURLs) == 0 {
			var err error
			// 创建能够定时更新 ConfigServerAddrs 的负载均衡器，赋值给 b
			b, err = NewAutoFetchBalancer(configServerURL, appID, options.ApolloClient.GetConfigServers, options.RefreshIntervalInSecond, options.Logger)
			if err != nil {
				return options, err
			}
		} else {
			// 如果明确指定了 ConfigServerAddrs 且没有开启 EnableSLB 则使用 configServerURLs 初始本地负载均衡器
			b = NewRoundRobin(configServerURLs)
		}
		// 设置负载均衡器
		options.Balancer = b
	}

	// 预加载的 namespaces
	if len(options.PreloadNamespaces) == 0 {
		options.PreloadNamespaces = []string{defaultNamespace}
	} else {
		if !stringInSlice(defaultNamespace, options.PreloadNamespaces) {
			PreloadNamespaces(defaultNamespace)(&options)
		}
	}

	return options, nil
}

/*
参考了java客户端实现
目前实现方式:
 0. 客户端显式传入ConfigServerURL
 2. Get from OS environment variable

未实现:
 1. Get from System Property
 3. Get from server.properties
https://github.com/ctripcorp/apollo/blob/master/apollo-client/src/main/java/com/ctrip/framework/apollo/internals/ConfigServiceLocator.java#L74
*/
// 本函数目的是从配置或者环境变量中获取 ConfigServerAddrs 。
func getConfigServers(configServerURL string) []string {
	var urls []string
	for _, url := range []string{
		configServerURL,
		os.Getenv("APOLLO_CONFIGSERVICE"),
	} {
		if url != "" {
			urls = splitCommaSeparatedURL(url)
			break
		}
	}

	return urls
}

type Option func(*Options)

func Cluster(cluster string) Option {
	return func(o *Options) {
		o.Cluster = cluster
	}
}

func DefaultNamespace(defaultNamespace string) Option {
	return func(o *Options) {
		o.DefaultNamespace = defaultNamespace
	}
}

func PreloadNamespaces(namespaces ...string) Option {
	return func(o *Options) {
		o.PreloadNamespaces = append(o.PreloadNamespaces, namespaces...)
	}
}

func WithApolloClient(c ApolloClient) Option {
	return func(o *Options) {
		o.ApolloClient = c
	}
}

func WithLogger(l Logger) Option {
	return func(o *Options) {
		o.Logger = l
	}
}

func AutoFetchOnCacheMiss() Option {
	return func(o *Options) {
		o.AutoFetchOnCacheMiss = true
	}
}

func LongPollerInterval(i time.Duration) Option {
	return func(o *Options) {
		o.LongPollerInterval = i
	}
}

func BackupFile(backupFile string) Option {
	return func(o *Options) {
		o.BackupFile = backupFile
	}
}

func FailTolerantOnBackupExists() Option {
	return func(o *Options) {
		o.FailTolerantOnBackupExists = true
	}
}

func EnableSLB(b bool) Option {
	return func(o *Options) {
		o.EnableSLB = b
	}
}

func WithBalancer(b Balancer) Option {
	return func(o *Options) {
		o.Balancer = b
	}
}

func ConfigServerRefreshIntervalInSecond(refreshIntervalInSecond time.Duration) Option {
	return func(o *Options) {
		o.RefreshIntervalInSecond = refreshIntervalInSecond
	}
}

//
type GetOptions struct {
	DefaultValue string		// 默认值
	Namespace    string		// namespace
}

func newGetOptions(opts ...GetOption) GetOptions {
	var getOpts GetOptions
	for _, opt := range opts {
		opt(&getOpts)
	}
	return getOpts
}

type GetOption func(*GetOptions)


// 设置 o.DefaultValue
func WithDefault(defVal string) GetOption {
	return func(o *GetOptions) {
		o.DefaultValue = defVal
	}
}

// 设置 o.Namespace
func WithNamespace(namespace string) GetOption {
	return func(o *GetOptions) {
		o.Namespace = namespace
	}
}
