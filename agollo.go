package agollo

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
)

var (
	defaultConfigFilePath = "app.properties"
	defaultConfigType     = "properties"
	defaultNotificationID = -1
	defaultWatchTimeout   = 500 * time.Millisecond
	defaultAgollo         Agollo
)

type Agollo interface {
	Start() <-chan *LongPollerError
	Stop()
	Get(key string, opts ...GetOption) string
	GetNameSpace(namespace string) Configurations
	Watch() <-chan *ApolloResponse
	WatchNamespace(namespace string, stop chan bool) <-chan *ApolloResponse
	Options() Options
}

type ApolloResponse struct {
	Namespace string
	OldValue  Configurations
	NewValue  Configurations
	Changes   Changes
	Error     error
}

type LongPollerError struct {
	ConfigServerURL string
	AppID           string
	Cluster         string
	Notifications   []Notification
	Namespace       string // 服务响应200后去非缓存接口拉取时的namespace
	Err             error
}

type agollo struct {


	opts Options


	notificationMap sync.Map // key: namespace value: notificationId

	releaseKeyMap   sync.Map // key: namespace value: releaseKey
	cache           sync.Map // key: namespace value: Configurations
	initialized     sync.Map // key: namespace value: bool

	watchCh             chan *ApolloResponse // watch all namespace
	watchNamespaceChMap sync.Map             // key: namespace value: chan *ApolloResponse

	errorsCh chan *LongPollerError

	runOnce  sync.Once
	stop     bool
	stopCh   chan struct{}
	stopLock sync.Mutex
}

func NewWithConfigFile(configFilePath string, opts ...Option) (Agollo, error) {


	// 打开配置文件
	f, err := os.Open(configFilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()


	// 解析配置
	var conf struct {
		AppID          string   `json:"appId,omitempty"`
		Cluster        string   `json:"cluster,omitempty"`
		NamespaceNames []string `json:"namespaceNames,omitempty"`
		IP             string   `json:"ip,omitempty"`
	}
	if err := json.NewDecoder(f).Decode(&conf); err != nil {
		return nil, err
	}

	// 调用 New 创建
	return New(
		conf.IP,
		conf.AppID,
		append(
			[]Option{
				Cluster(conf.Cluster),
				PreloadNamespaces(conf.NamespaceNames...),
			},
			opts...,
		)...,
	)
}

func New(configServerURL, appID string, opts ...Option) (Agollo, error) {

	a := &agollo{
		stopCh:   make(chan struct{}),
		errorsCh: make(chan *LongPollerError),
	}


	options, err := newOptions(configServerURL, appID, opts...)
	if err != nil {
		return nil, err
	}
	a.opts = options

	return a.preload()
}

func (a *agollo) preload() (Agollo, error) {


	//
	for _, namespace := range a.opts.PreloadNamespaces {

		err := a.initNamespace(namespace)
		if err != nil {
			return nil, err
		}

	}

	return a, nil
}



func (a *agollo) initNamespace(namespace string) error {

	// LoadOrStore() 参数是一对 <key, value>，如果该 key 存在且没有被标记删除则返回原先的 value（不更新）和 true；不存在则 store ，返回该 value 和 false。
	//
	// So:
	//  如果 namespace 存在，返回 found 为 true
	//  如果 namespace 不存在，就设置其值为 true 并返回 found 为 false
	_, found := a.initialized.LoadOrStore(namespace, true)

	if !found {
		// 如果 namespace 不存在，就加载它
		_, err := a.reloadNamespace(namespace, defaultNotificationID)
		return err
	}

	return nil
}


// 保存 <namespace, notificationID> 到 notificationMap 中
// 请求 config server 服务获取 namespace 的最新配置并保存到 cache 中
// 备份整个 cache 中的配置到文件中
func (a *agollo) reloadNamespace(namespace string, notificationID int) (conf Configurations, err error) {

	// 保存 <namespace, notificationID> 到 notificationMap 中
	a.notificationMap.Store(namespace, notificationID)

	// 取出一个 config server 的 url 地址
	var configServerURL string
	configServerURL, err = a.opts.Balancer.Select()
	if err != nil {
		return nil, err
	}

	var (
		status              int
		config              *Config
		cachedReleaseKey, _ = a.releaseKeyMap.LoadOrStore(namespace, "")
	)

	// 请求 config server 服务获取 namespace 的最新配置
	status, config, err = a.opts.ApolloClient.GetConfigsFromNonCache(  // 非缓存接口，
		configServerURL,						// config server addr
		a.opts.AppID,							// AppID
		a.opts.Cluster,							// 集群
		namespace,								// namespace
		ReleaseKey(cachedReleaseKey.(string)),	// 当前版本号
	)

	// 服务端未找到 namespace 时返回 404
	if status == http.StatusNotFound {
		// fix #23 当需要加载的 namespace 还未创建时，置为以下状态
		// 1. notificationMap 添加 namespace 以保证在服务器端的轮训列表中
		// 2. 且 notificationID(0)  > 默认值(-1), 服务端新建完后发送改变事件
		a.notificationMap.Store(namespace, 0)
		a.cache.Store(namespace, Configurations{})
		return
	}

	// 其它错误，意味着连接 config server 异常，检查是否需要读取本地备份文件
	if err != nil || status != http.StatusOK {

		a.log("ConfigServerUrl", configServerURL, "Namespace", namespace, "Action", "ReloadNameSpace", "Error", err)

		conf = Configurations{}

		// 异常状况下，如果开启容灾，则读取备份
		if a.opts.FailTolerantOnBackupExists {

			// 查找目标 namespace 的本地备份信息
			backupConfig, lerr := a.loadBackup(namespace)
			if lerr == nil {
				conf = backupConfig
				err = nil
			}
		}

		// 更新 namespace 的缓存
		a.cache.Store(namespace, conf)

		// 更新 namespace 的配置版本号
		a.releaseKeyMap.Store(namespace, cachedReleaseKey.(string))

		return
	}


	// 没有错误：
	// (1) 更新 namespace 的配置缓存
	// (2) 更新 namespace 的当前版本号
	conf = config.Configurations
	a.cache.Store(namespace, config.Configurations)     // 覆盖旧缓存
	a.releaseKeyMap.Store(namespace, config.ReleaseKey) // 存储最新的 release_key


	// 因为 a.cache 发生变化，备份整个配置
	if err = a.backup(); err != nil {
		return
	}

	return
}

func (a *agollo) Get(key string, opts ...GetOption) string {


	getOpts := newGetOptions(
		append(
			[]GetOption{
				WithNamespace(a.opts.DefaultNamespace),
			},
			opts...,
		)...,
	)



	val, found := a.GetNameSpace(getOpts.Namespace)[key]
	if !found {
		return getOpts.DefaultValue
	}


	v, _ := ToStringE(val)


	return v
}

func (a *agollo) GetNameSpace(namespace string) Configurations {
	config, found := a.cache.LoadOrStore(namespace, Configurations{})
	if !found && a.opts.AutoFetchOnCacheMiss {
		_ = a.initNamespace(namespace)
		return a.getNameSpace(namespace)
	}

	return config.(Configurations)
}

func (a *agollo) getNameSpace(namespace string) Configurations {
	v, ok := a.cache.Load(namespace)
	if !ok {
		return Configurations{}
	}
	return v.(Configurations)
}

func (a *agollo) Options() Options {
	return a.opts
}

// 启动goroutine去轮训apollo通知接口
func (a *agollo) Start() <-chan *LongPollerError {
	a.runOnce.Do(func() {
		go func() {
			timer := time.NewTimer(a.opts.LongPollerInterval)
			defer timer.Stop()

			for !a.shouldStop() {
				select {
				case <-timer.C:
					a.longPoll()
					timer.Reset(a.opts.LongPollerInterval)
				case <-a.stopCh:
					return
				}
			}
		}()
	})

	return a.errorsCh
}

func (a *agollo) Stop() {
	a.stopLock.Lock()
	defer a.stopLock.Unlock()
	if a.stop {
		return
	}

	if a.opts.Balancer != nil {
		a.opts.Balancer.Stop()
	}

	a.stop = true
	close(a.stopCh)
}

func (a *agollo) shouldStop() bool {
	select {
	case <-a.stopCh:
		return true
	default:
		return false
	}
}

func (a *agollo) Watch() <-chan *ApolloResponse {
	if a.watchCh == nil {
		a.watchCh = make(chan *ApolloResponse)
	}

	return a.watchCh
}

func fixWatchNamespace(namespace string) string {
	// fix: 传给apollo类似test.properties这种namespace
	// 通知回来的NamespaceName却没有.properties后缀，追加.properties后缀来修正此问题
	ext := path.Ext(namespace)
	if ext == "" {
		namespace = namespace + "." + defaultConfigType
	}
	return namespace
}

func (a *agollo) WatchNamespace(namespace string, stop chan bool) <-chan *ApolloResponse {
	watchNamespace := fixWatchNamespace(namespace)
	watchCh, exists := a.watchNamespaceChMap.LoadOrStore(watchNamespace, make(chan *ApolloResponse))
	if !exists {
		go func() {
			// 非预加载以外的namespace,初始化基础meta信息,否则没有longpoll
			err := a.initNamespace(namespace)
			if err != nil {
				watchCh.(chan *ApolloResponse) <- &ApolloResponse{
					Namespace: namespace,
					Error:     err,
				}
			}

			if stop != nil {
				<-stop
				a.watchNamespaceChMap.Delete(watchNamespace)
			}
		}()
	}

	return watchCh.(chan *ApolloResponse)
}

func (a *agollo) sendWatchCh(namespace string, oldVal, newVal Configurations) {


	changes := oldVal.Different(newVal)
	if len(changes) == 0 {
		return
	}

	resp := &ApolloResponse{
		Namespace: namespace,
		OldValue:  oldVal,
		NewValue:  newVal,
		Changes:   changes,
	}

	timer := time.NewTimer(defaultWatchTimeout)
	for _, watchCh := range a.getWatchChs(namespace) {
		select {
		case watchCh <- resp:

		case <-timer.C: // 防止创建全局监听或者某个namespace监听却不消费死锁问题
			timer.Reset(defaultWatchTimeout)
		}
	}
}

func (a *agollo) getWatchChs(namespace string) []chan *ApolloResponse {
	var chs []chan *ApolloResponse
	if a.watchCh != nil {
		chs = append(chs, a.watchCh)
	}

	watchNamespace := fixWatchNamespace(namespace)
	if watchNamespaceCh, found := a.watchNamespaceChMap.Load(watchNamespace); found {
		chs = append(chs, watchNamespaceCh.(chan *ApolloResponse))
	}

	return chs
}

func (a *agollo) sendErrorsCh(configServerURL string, notifications []Notification, namespace string, err error) {

	// 构造错误信息结构体
	longPollerError := &LongPollerError{
		ConfigServerURL: configServerURL,
		AppID:           a.opts.AppID,
		Cluster:         a.opts.Cluster,
		Notifications:   notifications,
		Namespace:       namespace,
		Err:             err,
	}

	// 写错误信息管道
	select {
	case a.errorsCh <- longPollerError:
	default:
	}
}

func (a *agollo) log(kvs ...interface{}) {
	a.opts.Logger.Log(
		append([]interface{}{
			"[Agollo]", "",
			"AppID", a.opts.AppID,
			"Cluster", a.opts.Cluster,
		},
			kvs...,
		)...,
	)
}

func (a *agollo) backup() error {

	// map<namespace, configs>
	backup := map[string]Configurations{}
	a.cache.Range(func(key, val interface{}) bool {
		k, _ := key.(string)
		conf, _ := val.(Configurations)
		backup[k] = conf
		return true
	})

	// 配置信息序列化
	data, err := json.Marshal(backup)
	if err != nil {
		return err
	}

	// 创建目录
	err = os.MkdirAll(filepath.Dir(a.opts.BackupFile), 0777)
	// 根据 err 判断目录是否存在，不存在则报错
	if err != nil && !os.IsExist(err) {
		return err
	}

	// 写文件
	return ioutil.WriteFile(a.opts.BackupFile, data, 0666)
}

func (a *agollo) loadBackup(specifyNamespace string) (Configurations, error) {

	// 检查备份文件是否存在
	if _, err := os.Stat(a.opts.BackupFile); err != nil {
		return nil, err
	}

	// 打开备份文件
	data, err := ioutil.ReadFile(a.opts.BackupFile)
	if err != nil {
		return nil, err
	}

	// 反序列化
	backup := map[string]Configurations{}
	err = json.Unmarshal(data, &backup)
	if err != nil {
		return nil, err
	}

	// 查找目标 namespace 对应的配置，找到则返回
	for namespace, configs := range backup {
		if namespace == specifyNamespace {
			return configs, nil
		}
	}

	// 没有找到目标 namespace ，直接返回
	return nil, nil
}




func (a *agollo) longPoll() {

	localNotifications := a.notifications()
	configServerURL, err := a.opts.Balancer.Select()
	if err != nil {
		a.log("ConfigServerUrl", configServerURL, "Notifications", Notifications(localNotifications).String(), "Error", err, "Action", "Balancer.Select")
		a.sendErrorsCh("", nil, "", err)
		return
	}

	status, notifications, err := a.opts.ApolloClient.Notifications(
		configServerURL,
		a.opts.AppID,
		a.opts.Cluster,
		localNotifications,
	)

	if err != nil {
		a.log("ConfigServerUrl", configServerURL, "Notifications", Notifications(localNotifications).String(), "Error", err, "Action", "LongPoll")
		a.sendErrorsCh(configServerURL, notifications, "", err)
		return
	}

	// 如果返回200，说明配置有变化，需要更新本地缓存

	if status == http.StatusOK {

		// 服务端判断没有改变，不会返回结果,这个时候不需要修改，遍历空数组跳过
		for _, notification := range notifications {

			// 从 cache 中取出 namespace 当前配置值
			oldValue := a.getNameSpace(notification.NamespaceName)

			// 是否发送变更事件
			isSendChange := a.isSendChange(notification.NamespaceName)

			// 更新 namespace 本地信息：
			// 	1. 保存 <namespace, notificationID> 到 notificationMap 中
			// 	2. 请求 config server 服务获取 namespace 的最新配置并保存到 cache 中
			// 	3. 备份整个 cache 中的配置到文件中
			newValue, err := a.reloadNamespace(notification.NamespaceName, notification.NotificationID)


			// 更新成功
			if err == nil {

				// 需要发送变更事件
				if isSendChange {
					// 将变更事件发送到 变更监听管道
					a.sendWatchCh(notification.NamespaceName, oldValue, newValue)
				}

			} else {
				// 更新失败，可能是网络原因获取 namespace 最新配置出错，则发送到 错误监听管道
				a.sendErrorsCh(configServerURL, notifications, notification.NamespaceName, err)
			}
		}
	}
}

func (a *agollo) isSendChange(namespace string) bool {
	v, ok := a.notificationMap.Load(namespace)
	return ok && v.(int) > defaultNotificationID
}

// 获取需要监听的 pairs<namespace, notificationId>
func (a *agollo) notifications() []Notification {
	var notifications []Notification
	a.notificationMap.Range(func(key, val interface{}) bool {
		k, _ := key.(string)
		v, _ := val.(int)
		notifications = append(notifications, Notification{k, v})
		return true
	})
	return notifications
}

func Init(configServerURL, appID string, opts ...Option) (err error) {
	defaultAgollo, err = New(configServerURL, appID, opts...)
	return
}

func InitWithConfigFile(configFilePath string, opts ...Option) (err error) {
	defaultAgollo, err = NewWithConfigFile(configFilePath, opts...)
	return
}

func InitWithDefaultConfigFile(opts ...Option) error {
	return InitWithConfigFile(defaultConfigFilePath, opts...)
}

func Start() <-chan *LongPollerError {
	return defaultAgollo.Start()
}

func Stop() {
	defaultAgollo.Stop()
}

func Get(key string, opts ...GetOption) string {
	return defaultAgollo.Get(key, opts...)
}

func GetNameSpace(namespace string) Configurations {
	return defaultAgollo.GetNameSpace(namespace)
}

func Watch() <-chan *ApolloResponse {
	return defaultAgollo.Watch()
}

func WatchNamespace(namespace string, stop chan bool) <-chan *ApolloResponse {
	return defaultAgollo.WatchNamespace(namespace, stop)
}

func GetAgollo() Agollo {
	return defaultAgollo
}
