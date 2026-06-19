package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

type Gateway struct {
	shardKey string
	leaders  []string
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

	g = Gateway{shardKey: key, leaders: leaders}
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
	// GET per ID: non conosciamo il tenant, cerchiamo su tutti gli shard
	for _, leader := range g.leaders {
		resp, err := http.Get(leader + "/get/" + id)
		if err != nil {
			continue
		}
		defer resp.Body.Close()
		var res map[string]any
		json.NewDecoder(resp.Body).Decode(&res)
		if _, hasErr := res["error"]; !hasErr {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(res)
			return
		}
	}
	writeErr(w, "not found")
}

func handleUpdate(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/update/")
	body, _ := io.ReadAll(r.Body)

	key := extractKey(body)
	if key != "" {
		if err := proxyPost(w, leaderFor(key)+"/update/"+id, body); err != nil {
			writeErr(w, err.Error())
		}
		return
	}

	for _, leader := range g.leaders {
		resp, err := http.Post(leader+"/update/"+id, "application/json", bytes.NewBuffer(body))
		if err != nil {
			continue
		}
		defer resp.Body.Close()
		var res map[string]any
		json.NewDecoder(resp.Body).Decode(&res)
		if res["status"] == "ok" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(res)
			return
		}
	}
	writeErr(w, "update failed on all shards")
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	var payload map[string]any
	json.Unmarshal(body, &payload)

	if where, ok := payload["where"].(map[string]any); ok {
		if val, ok := where[g.shardKey]; ok {
			keyVal := fmt.Sprint(val)
			leader := leaderFor(keyVal)
			if err := proxyPost(w, leader+"/query", body); err != nil {
				writeErr(w, err.Error())
			}
			return
		}
	}

	type shardResult struct {
		results []any
		count   float64
	}

	var allResults []any
	var totalCount float64

	for _, leader := range g.leaders {
		resp, err := http.Post(leader+"/query", "application/json", bytes.NewBuffer(body))
		if err != nil {
			continue
		}
		var res map[string]any
		json.NewDecoder(resp.Body).Decode(&res)
		resp.Body.Close()

		if results, ok := res["results"].([]any); ok {
			allResults = append(allResults, results...)
		}
		if c, ok := res["count"].(float64); ok {
			totalCount += c
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"count":   totalCount,
		"results": allResults,
	})
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

func main() {
	initGateway()

	port := os.Getenv("GATEWAY_PORT")
	if port == "" {
		port = "4000"
	}

	http.HandleFunc("/insert",   handleInsert)
	http.HandleFunc("/get/",     handleGet)
	http.HandleFunc("/update/",  handleUpdate)
	http.HandleFunc("/query",    handleQuery)
	http.HandleFunc("/save",     handleSave)
	http.HandleFunc("/load",     handleLoad)
	http.HandleFunc("/health",   handleHealth)

	fmt.Printf("[gateway] listening on :%s\n", port)
	http.ListenAndServe(":"+port, nil)
}