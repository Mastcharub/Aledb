package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
)

type Gateway struct {
	shardKey  string
	leaders   []string
	ring      Ring
	tenantsMu sync.RWMutex
	tenants   map[string]bool
}

var g Gateway

const tenantsFile = "data/tenants.txt"

func initGateway() {
	raw := os.Getenv("SHARD_LEADERS")
	if raw == "" {
		raw = "http://shard0-leader:3000"
	}
	leaders := strings.Split(raw, ",")
	for i, l := range leaders {
		leaders[i] = strings.TrimSpace(l)
	}
	key := os.Getenv("SHARD_KEY")
	if key == "" {
		key = "tenant_id"
	}
	ring := buildRing(leaders)
	g = Gateway{
    	shardKey: key,
    	leaders: leaders,
    	ring: ring,
    	tenants: make(map[string]bool),
	}

	if err := loadTenants(); err != nil {
    	fmt.Printf("[gateway] errore caricamento tenants: %v\n", err)
	}
	fmt.Printf("[gateway] %d shard(s), key=%q\n", len(g.leaders), g.shardKey)
}

const virtualNodes = 150

type ringEntry struct {
	hash   uint64
	leader string
}

type Ring struct {
	entries []ringEntry
}

func fnv1a(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

func buildRing(leaders []string) Ring {
	var entries []ringEntry
	for _, leader := range leaders {
		for i := 0; i < virtualNodes; i++ {
			key := fmt.Sprintf("%s#%d", leader, i)
			entries = append(entries, ringEntry{hash: fnv1a(key), leader: leader})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].hash < entries[j].hash
	})
	return Ring{entries: entries}
}

func (r *Ring) leaderFor(keyVal string) string {
	if len(r.entries) == 0 {
		return ""
	}
	h := fnv1a(keyVal)
	lo, hi := 0, len(r.entries)
	for lo < hi {
		mid := (lo + hi) / 2
		if r.entries[mid].hash < h {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return r.entries[lo%len(r.entries)].leader
}

func leaderFor(keyVal string) string {
	return g.ring.leaderFor(keyVal)
}

func extractKey(body []byte) string {
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		return ""
	}
	if v, ok := doc[g.shardKey]; ok {
		return fmt.Sprint(v)
	}
	return ""
}

func loadTenants() error {
	data, err := os.ReadFile(tenantsFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	lines := strings.Split(string(data), "\n")
	g.tenantsMu.Lock()
	defer g.tenantsMu.Unlock()

	for _, line := range lines {
		id := strings.TrimSpace(line)
		if id != "" {
			g.tenants[id] = true
		}
	}
	return nil
}

func registerTenant(id string) error {
	g.tenantsMu.Lock()
	defer g.tenantsMu.Unlock()

	if g.tenants[id] {
		return nil
	}

	f, err := os.OpenFile(tenantsFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.WriteString(id + "\n"); err != nil {
		return err
	}

	g.tenants[id] = true
	return nil
}

func isValidTenant(id string) bool {
	g.tenantsMu.RLock()
	defer g.tenantsMu.RUnlock()
	return g.tenants[id]
}

func migrateTenant(tenantID string) {
	newLeader := leaderFor(tenantID)
	client := &http.Client{}
	exportPayload, _ := json.Marshal(map[string]any{
		"shard_key": g.shardKey,
		"tenant_id": tenantID,
		"delete":    false,
	})

	var docs []any
	var sourceLeader string

	for _, leader := range g.leaders {
		if leader == newLeader {
			continue
		}
		resp, err := client.Post(leader+"/migrate/export", "application/json", bytes.NewBuffer(exportPayload))
		if err != nil {
			continue
		}
		var res map[string]any
		json.NewDecoder(resp.Body).Decode(&res)
		resp.Body.Close()
		if count, ok := res["count"].(float64); ok && count > 0 {
			docs, _ = res["docs"].([]any)
			sourceLeader = leader
			break
		}
	}

	if len(docs) == 0 {
		return
	}

	importPayload, _ := json.Marshal(map[string]any{"docs": docs})
	resp, err := client.Post(newLeader+"/migrate/import", "application/json", bytes.NewBuffer(importPayload))
	if err != nil {
		fmt.Printf("[migrate] import failed for %s: %v\n", tenantID, err)
		return
	}
	var importRes map[string]any
	json.NewDecoder(resp.Body).Decode(&importRes)
	resp.Body.Close()

	if imported, ok := importRes["imported"].(float64); ok && int(imported) == len(docs) {
		deletePayload, _ := json.Marshal(map[string]any{
			"shard_key": g.shardKey,
			"tenant_id": tenantID,
			"delete":    true,
		})
		client.Post(sourceLeader+"/migrate/export", "application/json", bytes.NewBuffer(deletePayload))
		fmt.Printf("[migrate] %s: %d docs %s → %s\n", tenantID, len(docs), sourceLeader, newLeader)
	}
}

func autoMigrate() {
	g.tenantsMu.RLock()
	tenants := make([]string, 0, len(g.tenants))
	for id := range g.tenants {
		tenants = append(tenants, id)
	}
	g.tenantsMu.RUnlock()

	for _, id := range tenants {
		migrateTenant(id)
	}
}

func proxyPost(w http.ResponseWriter, url string, body []byte) error {
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	_, err = io.Copy(w, resp.Body)
	return err
}

func proxyGet(w http.ResponseWriter, url string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	_, err = io.Copy(w, resp.Body)
	return err
}

func writeErr(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadGateway)
	fmt.Fprintf(w, `{"error":%q}`, msg)
}

func handleInsert(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	key := extractKey(body)
	if key == "" {
		writeErr(w, "campo shard_key mancante nel documento")
		return
	}
	if !isValidTenant(key) {
		writeErr(w, fmt.Sprintf("%s sconosciuto — registralo prima con POST /tenant/register", g.shardKey))
		return
	}
	if err := proxyPost(w, leaderFor(key)+"/insert", body); err != nil {
		writeErr(w, err.Error())
	}
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/get/")
	if id == "" {
		writeErr(w, "id mancante")
		return
	}
	tenant := r.URL.Query().Get(g.shardKey)
	if tenant == "" {
		writeErr(w, fmt.Sprintf("parametro ?%s mancante", g.shardKey))
		return
	}
	if err := proxyGet(w, leaderFor(tenant)+"/get/"+id); err != nil {
		writeErr(w, err.Error())
	}
}

func handleUpdate(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/update/")
	body, _ := io.ReadAll(r.Body)
	key := extractKey(body)
	if key == "" {
		key = r.URL.Query().Get(g.shardKey)
	}
	if key == "" {
		writeErr(w, fmt.Sprintf("%s mancante nel body o in ?%s", g.shardKey, g.shardKey))
		return
	}
	if err := proxyPost(w, leaderFor(key)+"/update/"+id, body); err != nil {
		writeErr(w, err.Error())
	}
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/delete/")
	if id == "" {
		writeErr(w, "id mancante")
		return
	}
	tenant := r.URL.Query().Get(g.shardKey)
	if tenant == "" {
		writeErr(w, fmt.Sprintf("parametro ?%s obbligatorio", g.shardKey))
		return
	}
	req, _ := http.NewRequest("DELETE", leaderFor(tenant)+"/delete/"+id, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		writeErr(w, err.Error())
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp.Body)
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var payload map[string]any
	json.Unmarshal(body, &payload)
	where, ok := payload["where"].(map[string]any)
	if !ok {
		writeErr(w, fmt.Sprintf("'where.%s' obbligatorio", g.shardKey))
		return
	}
	val, ok := where[g.shardKey]
	if !ok {
		writeErr(w, fmt.Sprintf("'where.%s' obbligatorio", g.shardKey))
		return
	}
	if err := proxyPost(w, leaderFor(fmt.Sprint(val))+"/query", body); err != nil {
		writeErr(w, err.Error())
	}
}

func handleSave(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var results []any
	for _, leader := range g.leaders {
		resp, err := http.Post(leader+"/save", "application/json", bytes.NewBuffer(body))
		if err != nil {
			continue
		}
		var res any
		json.NewDecoder(resp.Body).Decode(&res)
		resp.Body.Close()
		results = append(results, res)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"shards": results})
}

func handleLoad(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var results []any
	for _, leader := range g.leaders {
		resp, err := http.Post(leader+"/load", "application/json", bytes.NewBuffer(body))
		if err != nil {
			continue
		}
		var res any
		json.NewDecoder(resp.Body).Decode(&res)
		resp.Body.Close()
		results = append(results, res)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"shards": results})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	type shardHealth struct {
		Leader string `json:"leader"`
		Status any    `json:"status"`
	}
	var shards []shardHealth
	for _, leader := range g.leaders {
		sh := shardHealth{Leader: leader}
		resp, err := http.Get(leader + "/health")
		if err != nil {
			sh.Status = map[string]any{"error": err.Error()}
		} else {
			json.NewDecoder(resp.Body).Decode(&sh.Status)
			resp.Body.Close()
		}
		shards = append(shards, sh)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"gateway": "ok", "shards": shards})
}

func handleShardFor(w http.ResponseWriter, r *http.Request) {
	tenant := r.URL.Query().Get(g.shardKey)
	if tenant == "" {
		writeErr(w, fmt.Sprintf("parametro ?%s mancante", g.shardKey))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		g.shardKey: tenant,
		"leader":   leaderFor(tenant),
	})
}

func newTenantID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

func handleTenantRegister(w http.ResponseWriter, r *http.Request) {
	id, err := newTenantID()
	if err != nil {
		writeErr(w, "generazione tenant_id fallita")
		return
	}
	if err := registerTenant(id); err != nil {
    	writeErr(w, err.Error())
    	return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{g.shardKey: id})
}

func main() {
	initGateway()
	autoMigrate()

	port := os.Getenv("GATEWAY_PORT")
	if port == "" {
		port = "4000"
	}

	http.HandleFunc("/insert",          handleInsert)
	http.HandleFunc("/get/",            handleGet)
	http.HandleFunc("/update/",         handleUpdate)
	http.HandleFunc("/delete/",         handleDelete)
	http.HandleFunc("/query",           handleQuery)
	http.HandleFunc("/save",            handleSave)
	http.HandleFunc("/load",            handleLoad)
	http.HandleFunc("/tenant/register", handleTenantRegister)
	http.HandleFunc("/shard-for",       handleShardFor)
	http.HandleFunc("/health",          handleHealth)

	fmt.Printf("[gateway] listening on :%s\n", port)
	http.ListenAndServe(":"+port, nil)
}