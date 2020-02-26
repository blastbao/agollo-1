package agollo

import (
	"errors"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	defaultRefreshIntervalInSecond = time.Second * 60
	defaultMetaURL                 = "http://apollo.meta"
	ErrNoConfigServerAvailable     = errors.New("no config server availbale")
)

type Balancer interface {
	Select() (string, error)
	Stop()
}

// 定时更新 ConfigServerAddrs 的负载均衡器
type autoFetchBalancer struct {
	appID string
	getConfigServers GetConfigServersFunc
	metaServerAddress string				// MetaServerAddr
	logger Logger
	mu sync.RWMutex
	b  Balancer 							// 负载均衡器
	stopCh chan struct{}
}

// 根据
type GetConfigServersFunc func(metaServerURL, appID string) (int, []ConfigServer, error)

// 创建能够定时更新 ConfigServerAddrs 的负载均衡器
func NewAutoFetchBalancer(
	configServerURL string,
	appID string,
	getConfigServers GetConfigServersFunc,
	refreshIntervalInSecond time.Duration,
	logger Logger,
) (Balancer, error) {

	// 如果不指定，默认更新频率为 60s
	if refreshIntervalInSecond <= time.Duration(0) {
		refreshIntervalInSecond = defaultRefreshIntervalInSecond
	}

	b := &autoFetchBalancer{
		appID:             appID,
		getConfigServers:  getConfigServers,
		metaServerAddress: getMetaServerAddress(configServerURL), // Meta Server只是一个逻辑角色，在部署时和Config Service是在一个JVM进程中的，所以IP、端口和Config Service一致
		logger:            logger,
		stopCh:            make(chan struct{}),
		b:                 NewRoundRobin([]string{configServerURL}),
	}

	// 第一次获取 ConfigServerAddrs 并设置本地负载均衡器 b.b
	err := b.updateConfigServices()
	if err != nil {
		return nil, err
	}

	// 启动定时器，定时获取 ConfigServerAddrs 并更新本地负载均衡器 b.b
	go func() {
		ticker := time.NewTicker(refreshIntervalInSecond)
		defer ticker.Stop()

		for {
			select {
			case <-b.stopCh:
				return
			case <-ticker.C:
				_ = b.updateConfigServices()
			}
		}
	}()

	return b, nil
}

/*
参考了java客户端实现
目前实现方式:
0. 客户端显式传入ConfigServerURL
1. 读取APOLLO_META环境变量
2. 默认如果没有提供meta服务地址默认使用(http://apollo.meta)

未实现:
读取properties的逻辑
https://github.com/ctripcorp/apollo/blob/7545bd3cd7d4b996d7cda50f53cd4aa8b045a2bb/apollo-core/src/main/java/com/ctrip/framework/apollo/core/MetaDomainConsts.java#L27
*/

// 本函数目的是从配置或者环境变量中获取 MetaServerAddress 域名。
// 	MetaServerAddress 是一个域名， Agollo Client 通过该域名获取所有 Config Service 的 Pair<ip, port>，以便拉取配置。
// 	MetaServerAddress 可以由 `配置项` 或者 `环境变量` 指定。
func getMetaServerAddress(configServerURL string) string {

	var urls []string

	for _, url := range []string{
		configServerURL,          // 优先：显式传入的 ConfigServerURL 配置
		os.Getenv("APOLLO_META"), // 次之：APOLLO_META 环境变量
	} {

		if url != "" {
			urls = splitCommaSeparatedURL(url) // 逗号分割
			break
		}
	}

	// 随机返回 urls 中一条 url（通常只有一个 meta server 域名）
	if len(urls) > 0 {
		return normalizeURL(urls[rand.Intn(len(urls))])
	}

	return defaultMetaURL
}

func (b *autoFetchBalancer) updateConfigServices() error {

	// 1. 请求 MetaServer 获取 ConfigServers 的地址
	css, err := b.getConfigServices()
	if err != nil {
		return err
	}

	// 2. 检查 Config Server Addrs 中是否存在可用的 Config Server 地址
	var urls []string
	for _, url := range css {
		// check whether /services/config is accessible
		status, _, err := b.getConfigServers(url, b.appID)
		if err != nil {
			continue
		}

		// select the first available meta server
		// https://github.com/ctripcorp/apollo/blob/7545bd3cd7d4b996d7cda50f53cd4aa8b045a2bb/apollo-core/src/main/java/com/ctrip/framework/apollo/core/MetaDomainConsts.java#L166
		// 这里这段逻辑是参考java客户端，直接选了第一个可用的meta server
		if 200 <= status && status <= 399 {
			urls = append(urls, url)
			break
		}
	}

	// 3. 如果不存在可用地址，就直接返回，不更新负载均衡器
	if len(urls) == 0 {
		return nil
	}

	// 4. 如果存在可用地址，就更新负载均衡器
	b.mu.Lock()
	b.b = NewRoundRobin(css)
	b.mu.Unlock()

	return nil
}

// 请求 MetaServer 获取 ConfigServers 的地址
func (b *autoFetchBalancer) getConfigServices() ([]string, error) {

	// 根据 MetaServerAddr 和 AppID 获取一组 Config Server 对象
	_, css, err := b.getConfigServers(b.metaServerAddress, b.appID)

	if err != nil {
		b.logger.Log("[Agollo]", "", "AppID", b.appID, "MetaServerAddress", b.metaServerAddress, "Error", err)
		return nil, err
	}

	// 把这组 Config Server 对象的地址抽取出来，转换成字符串数组
	var urls []string
	for _, cs := range css {
		urls = append(urls, normalizeURL(cs.HomePageURL))
	}

	// 返回这组 Config Servers 的地址
	return urls, nil
}

func (b *autoFetchBalancer) Select() (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.b.Select()
}

func (b *autoFetchBalancer) Stop() {
	close(b.stopCh)
}

type roundRobin struct {
	ss []string
	c  uint64
}




// 轮训选择器
func NewRoundRobin(ss []string) Balancer {
	return &roundRobin{
		ss: ss,
		c:  0,
	}
}

func (rr *roundRobin) Select() (string, error) {
	if len(rr.ss) <= 0 {
		return "", ErrNoConfigServerAvailable
	}

	old := atomic.AddUint64(&rr.c, 1) - 1
	idx := old % uint64(len(rr.ss))
	return rr.ss[idx], nil
}

func (rr *roundRobin) Stop() {

}
