package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// 内存存储层
type cacheItem struct {
	value     string
	expiresAt time.Time // 零值代表永不过期
}

func (item cacheItem) isExpired() bool {
	if item.expiresAt.IsZero() {
		return false
	}
	return time.Now().After(item.expiresAt)
}

type MemoryStore struct {
	data map[string]cacheItem
	mu   sync.RWMutex
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[string]cacheItem),
	}
}

// Set 设置键值，支持传入过期时长 （duration<=0 表示永不过期）
func (s *MemoryStore) Set(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	s.data[key] = cacheItem{
		value:     value,
		expiresAt: exp,
	}
}

// Get 获取值(采用惰性删除策略：读到已过期的可以时自动删除)
func (s *MemoryStore) Get(key string) (string, bool) {
	s.mu.RLock()
	item, ok := s.data[key]
	s.mu.RUnlock()

	if !ok {
		return "", false
	}

	if item.isExpired() {
		s.mu.Lock()
		delete(s.data, key)
		s.mu.Unlock()
		return "", false
	}

	return item.value, true

}

// 批量删除 key，返回删除的 key 数量
func (s *MemoryStore) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for _, k := range keys {
		if _, ok := s.data[k]; ok {
			delete(s.data, k)
			count++
		}
	}
	return count
}

// TTL 获取剩余生存时间（秒）：-2=不存在 -1=永不过期，>=0=剩余秒数
func (s *MemoryStore) TTL(key string) int64 {
	s.mu.RLock()
	item, ok := s.data[key]
	s.mu.RUnlock()

	if !ok {
		return -2
	}
	if item.isExpired() {
		s.mu.Lock()
		delete(s.data, key)
		s.mu.Unlock()
		return -2
	}
	if item.expiresAt.IsZero() {
		return -1
	}
	return int64(time.Until(item.expiresAt).Seconds())
}

// PeriodicCleanup 定期主动扫描清理过期 key（后台守护协程）
func (s *MemoryStore) StartCleanupDaemon(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[存储层] 后台清理守护协程已停止")
			return
		case <-ticker.C:
			s.cleanExpiredKeys()
		}
	}

}

// 内部方法：扫描清理过期
func (s *MemoryStore) cleanExpiredKeys() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	deleted := 0
	for k, v := range s.data {
		if !v.expiresAt.IsZero() && now.After(v.expiresAt) {
			delete(s.data, k)
			deleted++
		}
	}
	if deleted > 0 {
		log.Printf("[存储层] 清理了 %d 个过期 key", deleted)
	}
}

// miniredis 协议层
func parseCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return nil, errors.New("empty command")
	}

	// 如果是 RESP 数组格式
	if line[0] == '*' {
		numArgs, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}

		args := make([]string, 0, numArgs)
		for i := 0; i < numArgs; i++ {
			lenLine, err := reader.ReadString('\n')
			if err != nil {
				return nil, err
			}
			lenLine = strings.TrimRight(lenLine, "\r\n")
			if len(lenLine) == 0 || lenLine[0] != '$' {
				return nil, errors.New("protocol error: expected $")
			}
			strLen, err := strconv.Atoi(lenLine[1:])
			if err != nil {
				return nil, err
			}

			//读取实际业务数据
			buf := make([]byte, strLen+2)
			_, err = io.ReadFull(reader, buf)
			if err != nil {
				return nil, err
			}
			args = append(args, string(buf[:strLen]))
		}
		return args, nil
	}
	// 普通文本格式
	return strings.Fields(line), nil
}

// 命令分发处理器
type RedisServer struct {
	store *MemoryStore
	wg    sync.WaitGroup
}

func (srv *RedisServer) handleCommand(args []string) string {
	if len(args) == 0 {
		return "-ERR empty command\r\n"
	}
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "PING":
		if len(args) > 1 {
			msg := args[1]
			return fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)
		}
		return "+PONG\r\n"

	case "SET":
		//支持：SET key value [EX seconfs]
		if len(args) < 3 {
			return "-ERR wrong number of arguments for 'set' command\r\n"
		}
		key, val := args[1], args[2]
		var ttl time.Duration

		if len(args) >= 5 && strings.ToUpper(args[3]) == "EX" {
			Seconds, err := strconv.Atoi(args[4])
			if err != nil || Seconds <= 0 {
				return "-ERR invalid expire time in 'set' command\r\n"
			}
			ttl = time.Duration(Seconds) * time.Second
		}
		srv.store.Set(key, val, ttl)
		return "+OK\r\n"
	case "GET":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for 'get' command\r\n"
		}
		val, exists := srv.store.Get(args[1])
		if !exists {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
	case "DEL":
		if len(args) < 2 {
			return "-ERR wrong number of arguments for 'del' command\r\n"
		}
		deletedCount := srv.store.Del(args[1:]...)
		return fmt.Sprintf(":%d\r\n", deletedCount)
	case "TTL":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for 'ttl' command\r\n"
		}
		ttl := srv.store.TTL(args[1])
		return fmt.Sprintf(":%d\r\n", ttl)
	default:
		return fmt.Sprintf("-ERR unknown command '%s'\r\n", cmd)
	}
}

// 处理单个客户端 TCP 连接
func (srv *RedisServer) handleConnection(ctx context.Context, conn net.Conn) {
	defer func() {
		conn.Close()
		srv.wg.Done()
	}()
	remoteAddr := conn.RemoteAddr().String()
	log.Printf("客户端连接接入: %s", remoteAddr)
	reader := bufio.NewReader(conn)

	for {
		//检查服务是否正在关闭
		select {
		case <-ctx.Done():
			conn.Write([]byte("-ERR server shutting down\r\n"))
			return
		default:
		}

		//设置读取超时，防止挂死连接无法检查退出信号
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		args, err := parseCommand(reader)

		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			if err != io.EOF {
				log.Printf("客户端 %s 断开连接: %v", remoteAddr, err)
			}
			return
		}

		conn.SetReadDeadline(time.Time{})
		resp := srv.handleCommand(args)
		if _, err := conn.Write(([]byte(resp))); err != nil {
			return
		}

	}
}

func main() {
	port := ":6380"
	listenter, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("❌ 监听端口 %s 失败: %v", port, err)
	}
	store := NewMemoryStore()
	server := &RedisServer{store: store}

	// 根 Context，用于广播系统退出信号
	ctx, canncel := context.WithCancel(context.Background())
	defer canncel()

	// 启动后台定时扫描清理协程（每 2 秒清理一次过期 key）
	go store.StartCleanupDaemon(ctx, 2*time.Second)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("\n收到信号 [%v]，正在执行退出...", sig)
		canncel()
		listenter.Close()
	}()
	fmt.Println("==================================================")
	fmt.Println("   🚀 Mini-Redis 缓存服务已成功启动！")
	fmt.Printf("   监听地址: 127.0.0.1%s\n", port)
	fmt.Println("==================================================")
	fmt.Println("📌 快速体验命令 (新开终端运行):")
	fmt.Println("   1. redis-cli 体验:")
	fmt.Println("      redis-cli -p 6380")
	fmt.Println("      > PING")
	fmt.Println("      > SET name \"Go\" EX 5")
	fmt.Println("      > GET name")
	fmt.Println("      > TTL name")
	fmt.Println("   2. 或者使用 nc / telnet:")
	fmt.Println("      nc 127.0.0.1 6380")
	fmt.Println("      PING")
	fmt.Println("      SET score 100")
	fmt.Println("      GET score")
	fmt.Println("==================================================")

	for {
		conn, err := listenter.Accept()

		if err != nil {
			select {
			case <-ctx.Done():
				log.Println("TCP 监听已关闭，等待现有连接断开...")
				server.wg.Wait()
				log.Println("所有连接已退出，Mini-Redis 正常关机！")
				return
			default:
				log.Printf("接收连接错误: %v", err)
				continue
			}
		}

		server.wg.Add(1)
		go server.handleConnection(ctx, conn)
	}

}
