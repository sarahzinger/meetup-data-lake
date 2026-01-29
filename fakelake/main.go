package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Row map[string]any

type Dataset struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Fields      []string `json:"fields"`
}

var datasets = []Dataset{
	{
		Name:        "events",
		Description: "Synthetic event stream (like clickstream/log events) stored as rows",
		Fields:      []string{"ts", "service", "region", "user_id", "event", "latency_ms", "status"},
	},
	{
		Name:        "metrics",
		Description: "Synthetic timeseries-like table (one row per sample)",
		Fields:      []string{"ts", "metric", "service", "region", "value"},
	},
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthz)
	mux.HandleFunc("/datasets", listDatasets)
	mux.HandleFunc("/query/", queryDataset) // /query/{dataset}

	addr := ":8080"
	log.Printf("fake lake listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, cors(mux)))
}

func healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func listDatasets(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"datasets": datasets,
	})
}

func queryDataset(w http.ResponseWriter, r *http.Request) {
	// Path: /query/{dataset}
	path := strings.TrimPrefix(r.URL.Path, "/query/")
	dataset := strings.Trim(path, "/")
	if dataset == "" {
		http.Error(w, "missing dataset in path: /query/{dataset}", http.StatusBadRequest)
		return
	}
	if !datasetExists(dataset) {
		http.Error(w, "unknown dataset", http.StatusNotFound)
		return
	}

	q := r.URL.Query()
	format := strings.ToLower(q.Get("format"))
	if format == "" {
		format = "json"
	}
	limit := parseIntDefault(q.Get("limit"), 200)
	offset := parseIntDefault(q.Get("offset"), 0)
	sortField := q.Get("sort")
	order := strings.ToLower(q.Get("order"))
	if order == "" {
		order = "asc"
	}

	// Time range (optional): from/to RFC3339
	var (
		from time.Time
		to   time.Time
		err  error
	)
	fromStr := q.Get("from")
	toStr := q.Get("to")
	if fromStr != "" {
		from, err = time.Parse(time.RFC3339, fromStr)
		if err != nil {
			http.Error(w, "invalid from (use RFC3339)", http.StatusBadRequest)
			return
		}
	}
	if toStr != "" {
		to, err = time.Parse(time.RFC3339, toStr)
		if err != nil {
			http.Error(w, "invalid to (use RFC3339)", http.StatusBadRequest)
			return
		}
	}

	// Parse filters: filter=field:value (repeatable)
	filters := parseFilters(q["filter"])

	// Generate synthetic rows (acts like reading from a lake table)
	// We generate a fixed "window" and then filter/slice it.
	seed := int64(1) // stable results across restarts
	rows := generateRows(dataset, seed, 5000)

	// Apply time filter if provided
	if !from.IsZero() || !to.IsZero() {
		rows = filterByTime(rows, from, to)
	}

	// Apply field filters
	if len(filters) > 0 {
		rows = filterByFields(rows, filters)
	}

	total := len(rows)

	// Sort if requested
	if sortField != "" {
		sortRows(rows, sortField, order)
	}

	// Pagination
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 200
	}
	start := min(offset, len(rows))
	end := min(offset+limit, len(rows))
	page := rows[start:end]

	// Response
	switch format {
	case "csv":
		writeCSV(w, dataset, page, total, limit, offset)
	case "json":
		fallthrough
	default:
		writeJSON(w, http.StatusOK, map[string]any{
			"dataset": dataset,
			"meta": map[string]any{
				"total":  total,
				"limit":  limit,
				"offset": offset,
			},
			"rows": page,
		})
	}
}

func datasetExists(name string) bool {
	for _, d := range datasets {
		if d.Name == name {
			return true
		}
	}
	return false
}

func generateRows(dataset string, seed int64, n int) []Row {
	rng := rand.New(rand.NewSource(seed))
	now := time.Now().UTC()

	services := []string{"api", "worker", "frontend", "billing"}
	regions := []string{"us-east-1", "us-west-2", "eu-west-1"}
	events := []string{"login", "purchase", "search", "logout"}
	metrics := []string{"cpu_util", "mem_rss_mb", "req_per_sec", "p95_latency_ms"}

	rows := make([]Row, 0, n)
	for i := 0; i < n; i++ {
		// Spread timestamps across last 7 days
		ts := now.Add(-time.Duration(rng.Intn(7*24*3600)) * time.Second).Truncate(time.Second)

		switch dataset {
		case "events":
			svc := services[rng.Intn(len(services))]
			reg := regions[rng.Intn(len(regions))]
			ev := events[rng.Intn(len(events))]
			status := 200
			if rng.Float64() < 0.08 {
				status = 500
			}
			latency := 20 + rng.Intn(500)

			rows = append(rows, Row{
				"ts":         ts.Format(time.RFC3339),
				"service":    svc,
				"region":     reg,
				"user_id":    fmt.Sprintf("u-%06d", rng.Intn(20000)),
				"event":      ev,
				"latency_ms": latency,
				"status":     status,
			})

		case "metrics":
			svc := services[rng.Intn(len(services))]
			reg := regions[rng.Intn(len(regions))]
			m := metrics[rng.Intn(len(metrics))]
			val := rng.Float64() * 100

			rows = append(rows, Row{
				"ts":      ts.Format(time.RFC3339),
				"metric":  m,
				"service": svc,
				"region":  reg,
				"value":   round(val, 3),
			})
		}
	}
	return rows
}

func filterByTime(rows []Row, from, to time.Time) []Row {
	out := make([]Row, 0, len(rows))
	for _, r := range rows {
		tsStr, ok := r["ts"].(string)
		if !ok {
			continue
		}
		ts, err := time.Parse(time.RFC3339, tsStr)
		if err != nil {
			continue
		}
		if !from.IsZero() && ts.Before(from) {
			continue
		}
		if !to.IsZero() && ts.After(to) {
			continue
		}
		out = append(out, r)
	}
	return out
}

func parseFilters(vals []string) map[string]string {
	m := map[string]string{}
	for _, v := range vals {
		// field:value
		parts := strings.SplitN(v, ":", 2)
		if len(parts) != 2 {
			continue
		}
		field := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if field != "" && value != "" {
			m[field] = value
		}
	}
	return m
}

func filterByFields(rows []Row, filters map[string]string) []Row {
	out := make([]Row, 0, len(rows))
	for _, r := range rows {
		match := true
		for k, v := range filters {
			rv, ok := r[k]
			if !ok {
				match = false
				break
			}
			// string compare for simplicity (Infinity-friendly)
			if fmt.Sprint(rv) != v {
				match = false
				break
			}
		}
		if match {
			out = append(out, r)
		}
	}
	return out
}

func sortRows(rows []Row, field, order string) {
	asc := order != "desc"
	sort.SliceStable(rows, func(i, j int) bool {
		a, aok := rows[i][field]
		b, bok := rows[j][field]
		if !aok && !bok {
			return false
		}
		if !aok {
			return asc
		}
		if !bok {
			return !asc
		}

		// Try time sort if field is "ts"
		if field == "ts" {
			at, errA := time.Parse(time.RFC3339, fmt.Sprint(a))
			bt, errB := time.Parse(time.RFC3339, fmt.Sprint(b))
			if errA == nil && errB == nil {
				if asc {
					return at.Before(bt)
				}
				return at.After(bt)
			}
		}

		// Try numeric
		af, errA := toFloat(a)
		bf, errB := toFloat(b)
		if errA == nil && errB == nil {
			if asc {
				return af < bf
			}
			return af > bf
		}

		// Fallback string
		as := fmt.Sprint(a)
		bs := fmt.Sprint(b)
		if asc {
			return as < bs
		}
		return as > bs
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeCSV(w http.ResponseWriter, dataset string, rows []Row, total, limit, offset int) {
	// For CSV, we put metadata in headers (handy for debugging)
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("X-Total", strconv.Itoa(total))
	w.Header().Set("X-Limit", strconv.Itoa(limit))
	w.Header().Set("X-Offset", strconv.Itoa(offset))

	// Choose columns based on dataset definition
	cols := []string{}
	for _, d := range datasets {
		if d.Name == dataset {
			cols = append(cols, d.Fields...)
			break
		}
	}
	if len(cols) == 0 {
		http.Error(w, "no fields for dataset", http.StatusInternalServerError)
		return
	}

	cw := csv.NewWriter(w)
	_ = cw.Write(cols)
	for _, r := range rows {
		rec := make([]string, 0, len(cols))
		for _, c := range cols {
			rec = append(rec, fmt.Sprint(r[c]))
		}
		_ = cw.Write(rec)
	}
	cw.Flush()
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simple permissive CORS so Grafana/Infinity can call it easily.
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func parseIntDefault(s string, def int) int {
	if s == "" {
		return def
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return i
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func toFloat(v any) (float64, error) {
	switch t := v.(type) {
	case float64:
		return t, nil
	case float32:
		return float64(t), nil
	case int:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case json.Number:
		return t.Float64()
	default:
		// try parse from string
		return strconv.ParseFloat(fmt.Sprint(v), 64)
	}
}

func round(f float64, decimals int) float64 {
	pow := 1.0
	for i := 0; i < decimals; i++ {
		pow *= 10
	}
	return float64(int(f*pow+0.5)) / pow
}
