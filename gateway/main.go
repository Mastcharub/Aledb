package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
)

type Gateway struct {
	shardKey string
	leaders  []string
	tenantsMu sync.RWMutex
	tenants   map[string]bool
}

var g Gateway

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

	g = Gateway{shardKey: key, leaders: leaders, tenants: make(map[string]bool)}
	fmt.Printf("[gateway] %d shard(s), key=%q\n", len(g.leaders), g.shardKey)
}

func fnv1a(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

func leaderFor(keyVal string) string {
	return g.leaders[fnv1a(keyVal)%uint64(len(g.leaders))]
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

func registerTenant(id string) {
	g.tenantsMu.Lock()
	defer g.tenantsMu.Unlock()
	g.tenants[id] = true
}

func isValidTenant(id string) bool {
	g.tenantsMu.RLock()
	defer g.tenantsMu.RUnlock()
	return g.tenants[id]
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
		writeErr(w, fmt.Sprintf("parametro ?%s mancante — obbligatorio per isolare i tenant", g.shardKey))
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
		writeErr(w, fmt.Sprintf("%s mancante nel body o in ?%s — obbligatorio per isolare i tenant", g.shardKey, g.shardKey))
		return
	}

	if err := proxyPost(w, leaderFor(key)+"/update/"+id, body); err != nil {
		writeErr(w, err.Error())
	}
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	var payload map[string]any
	json.Unmarshal(body, &payload)

	where, ok := payload["where"].(map[string]any)
	if !ok {
		writeErr(w, fmt.Sprintf("'where.%s' obbligatorio — isola i risultati al tuo tenant", g.shardKey))
		return
	}

	val, ok := where[g.shardKey]
	if !ok {
		writeErr(w, fmt.Sprintf("'where.%s' obbligatorio — isola i risultati al tuo tenant", g.shardKey))
		return
	}

	keyVal := fmt.Sprint(val)
	leader := leaderFor(keyVal)
	if err := proxyPost(w, leader+"/query", body); err != nil {
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
	json.NewEncoder(w).Encode(map[string]any{
		"gateway": "ok",
		"shards":  shards,
	})
}

func handleShardFor(w http.ResponseWriter, r *http.Request) {
	tenant := r.URL.Query().Get(g.shardKey)
	if tenant == "" {
		writeErr(w, fmt.Sprintf("parametro ?%s mancante", g.shardKey))
		return
	}
	idx := fnv1a(tenant) % uint64(len(g.leaders))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		g.shardKey:    tenant,
		"shard_index": idx,
		"leader":      g.leaders[idx],
	})
}

func newTenantID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC 4122
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

func handleTenantRegister(w http.ResponseWriter, r *http.Request) {
	id, err := newTenantID()
	if err != nil {
		writeErr(w, "generazione tenant_id fallita")
		return
	}
	registerTenant(id)
	idx := fnv1a(id) % uint64(len(g.leaders))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		g.shardKey:    id,
		"shard_index": idx,
		"leader":      g.leaders[idx],
	})
}

func main() {
	initGateway()

	port := os.Getenv("GATEWAY_PORT")
	if port == "" {
		port = "4000"
	}

	http.HandleFunc("/insert",   			handleInsert)
	http.HandleFunc("/get/",     			handleGet)
	http.HandleFunc("/update/",  			handleUpdate)
	http.HandleFunc("/query",    			handleQuery)
	http.HandleFunc("/save",     			handleSave)
	http.HandleFunc("/load",     			handleLoad)
	http.HandleFunc("/tenant/register", 	handleTenantRegister)
	http.HandleFunc("/shard-for", 			handleShardFor)
	http.HandleFunc("/health",   			handleHealth)

	fmt.Printf("[gateway] listening on :%s\n", port)
	http.ListenAndServe(":"+port, nil)
}