package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	staleTimeout    = 15 * time.Minute
	cleanupInterval = 1 * time.Minute
)

// Heartbeat is the JSON payload reflectors POST.
type Heartbeat struct {
	InstallID string `json:"install_id"`
	Version   string `json:"version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	Uptime    int64  `json:"uptime_seconds"`
	Reflector struct {
		Name       string `json:"name"`
		Callsign   string `json:"callsign"`
		Port       int    `json:"port"`
		Designator int    `json:"designator,omitempty"`
	} `json:"reflector"`
	Rooms          []RoomPayload `json:"rooms"`
	Clients        int           `json:"clients"`
	Links          int           `json:"links"`
	LinksActive    int           `json:"links_active"`
	Transmissions  int64         `json:"transmissions"`
	ExternalIP     string        `json:"external_ip,omitempty"`
}

type RoomPayload struct {
	DGID    int    `json:"dgid"`
	Name    string `json:"room_name"`
	Static  bool   `json:"static"`
	Clients int    `json:"clients"`
	Links   int    `json:"links"`
}

// reflectorState holds the last heartbeat from a reflector.
type reflectorState struct {
	heartbeat  Heartbeat
	lastSeen   time.Time
}

// Collector implements prometheus.Collector, generating metrics from
// the current set of known reflectors on each Prometheus scrape.
type Collector struct {
	mu          sync.RWMutex
	reflectors  map[string]*reflectorState // keyed by install_id

	descUp              *prometheus.Desc
	descUptime          *prometheus.Desc
	descClients         *prometheus.Desc
	descRooms           *prometheus.Desc
	descLinks           *prometheus.Desc
	descLinksActive     *prometheus.Desc
	descTransmissions   *prometheus.Desc
	descRoomClients     *prometheus.Desc
	descRoomLinks       *prometheus.Desc
	descReflectorsTotal *prometheus.Desc
}

func NewCollector() *Collector {
	labels := []string{"install_id", "name", "callsign", "version", "os", "arch"}
	roomLabels := append(labels, "dgid", "room_name")

	return &Collector{
		reflectors: make(map[string]*reflectorState),

		descUp:              prometheus.NewDesc("openrig_reflector_up", "Whether the reflector is reporting (1=active, 0=stale).", labels, nil),
		descUptime:          prometheus.NewDesc("openrig_reflector_uptime_seconds", "Reflector process uptime in seconds.", labels, nil),
		descClients:         prometheus.NewDesc("openrig_reflector_clients_total", "Number of connected clients.", labels, nil),
		descRooms:           prometheus.NewDesc("openrig_reflector_rooms_total", "Number of rooms.", labels, nil),
		descLinks:           prometheus.NewDesc("openrig_reflector_links_total", "Number of configured links.", labels, nil),
		descLinksActive:     prometheus.NewDesc("openrig_reflector_links_active", "Number of active links.", labels, nil),
		descTransmissions:   prometheus.NewDesc("openrig_reflector_transmissions_total", "Cumulative transmission count.", labels, nil),
		descRoomClients:     prometheus.NewDesc("openrig_reflector_room_clients", "Number of clients in a room.", roomLabels, nil),
		descRoomLinks:       prometheus.NewDesc("openrig_reflector_room_links", "Number of links in a room.", roomLabels, nil),
		descReflectorsTotal: prometheus.NewDesc("openrig_reflectors_total", "Total number of known reflectors.", nil, nil),
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.descUp
	ch <- c.descUptime
	ch <- c.descClients
	ch <- c.descRooms
	ch <- c.descLinks
	ch <- c.descLinksActive
	ch <- c.descTransmissions
	ch <- c.descRoomClients
	ch <- c.descRoomLinks
	ch <- c.descReflectorsTotal
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	now := time.Now()
	activeCount := 0

	for _, rs := range c.reflectors {
		hb := rs.heartbeat
		labels := []string{hb.InstallID, hb.Reflector.Name, hb.Reflector.Callsign, hb.Version, hb.OS, hb.Arch}

		up := float64(1)
		if now.Sub(rs.lastSeen) > staleTimeout {
			up = 0
		} else {
			activeCount++
		}

		ch <- prometheus.MustNewConstMetric(c.descUp, prometheus.GaugeValue, up, labels...)
		ch <- prometheus.MustNewConstMetric(c.descUptime, prometheus.GaugeValue, float64(hb.Uptime), labels...)
		ch <- prometheus.MustNewConstMetric(c.descClients, prometheus.GaugeValue, float64(hb.Clients), labels...)
		ch <- prometheus.MustNewConstMetric(c.descRooms, prometheus.GaugeValue, float64(len(hb.Rooms)), labels...)
		ch <- prometheus.MustNewConstMetric(c.descLinks, prometheus.GaugeValue, float64(hb.Links), labels...)
		ch <- prometheus.MustNewConstMetric(c.descLinksActive, prometheus.GaugeValue, float64(hb.LinksActive), labels...)
		ch <- prometheus.MustNewConstMetric(c.descTransmissions, prometheus.CounterValue, float64(hb.Transmissions), labels...)

		for _, room := range hb.Rooms {
			roomLabels := append(labels, itoa(room.DGID), room.Name)
			ch <- prometheus.MustNewConstMetric(c.descRoomClients, prometheus.GaugeValue, float64(room.Clients), roomLabels...)
			ch <- prometheus.MustNewConstMetric(c.descRoomLinks, prometheus.GaugeValue, float64(room.Links), roomLabels...)
		}
	}

	ch <- prometheus.MustNewConstMetric(c.descReflectorsTotal, prometheus.GaugeValue, float64(activeCount))
}

// Record updates the state for a reflector.
func (c *Collector) Record(hb Heartbeat) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reflectors[hb.InstallID] = &reflectorState{
		heartbeat: hb,
		lastSeen:  time.Now(),
	}
}

// Cleanup removes reflectors that haven't reported in a long time (4x stale timeout).
func (c *Collector) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := time.Now().Add(-4 * staleTimeout)
	for id, rs := range c.reflectors {
		if rs.lastSeen.Before(cutoff) {
			delete(c.reflectors, id)
			log.Printf("cleanup: removed stale reflector %s (%s)", rs.heartbeat.Reflector.Name, id)
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func main() {
	collector := NewCollector()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collector)

	// Cleanup stale reflectors periodically.
	go func() {
		ticker := time.NewTicker(cleanupInterval)
		defer ticker.Stop()
		for range ticker.C {
			collector.Cleanup()
		}
	}()

	mux := http.NewServeMux()

	// Heartbeat endpoint — reflectors POST here.
	mux.HandleFunc("POST /v1/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var hb Heartbeat
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&hb); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if hb.InstallID == "" {
			http.Error(w, "install_id required", http.StatusBadRequest)
			return
		}
		collector.Record(hb)
		log.Printf("heartbeat: %s (%s) v%s — %d clients, %d rooms",
			hb.Reflector.Name, hb.InstallID[:8], hb.Version, hb.Clients, len(hb.Rooms))
		w.WriteHeader(http.StatusNoContent)
	})

	// Reflector directory — public API for listing active reflectors.
	mux.HandleFunc("GET /v1/reflectors", func(w http.ResponseWriter, r *http.Request) {
		collector.mu.RLock()
		defer collector.mu.RUnlock()

		now := time.Now()
		type reflectorEntry struct {
			Name       string        `json:"name"`
			Callsign   string        `json:"callsign"`
			Designator int           `json:"designator,omitempty"`
			Host       string        `json:"host,omitempty"`
			Port       int           `json:"port"`
			Clients    int           `json:"clients"`
			Rooms      []RoomPayload `json:"rooms"`
			Version    string        `json:"version"`
		}
		var list []reflectorEntry
		for _, rs := range collector.reflectors {
			if now.Sub(rs.lastSeen) > staleTimeout {
				continue
			}
			hb := rs.heartbeat
			if hb.Reflector.Designator == 0 || hb.ExternalIP == "" {
				continue
			}
			list = append(list, reflectorEntry{
				Name:       hb.Reflector.Name,
				Callsign:   hb.Reflector.Callsign,
				Designator: hb.Reflector.Designator,
				Host:       hb.ExternalIP,
				Port:       hb.Reflector.Port,
				Clients:    hb.Clients,
				Rooms:      hb.Rooms,
				Version:    hb.Version,
			})
		}
		if list == nil {
			list = []reflectorEntry{}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(list)
	})

	// Prometheus scrape endpoint.
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	// Health check.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	addr := ":8090"
	log.Printf("openRigMetrics listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
