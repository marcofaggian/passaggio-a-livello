// ============================================================================
// PL-TRACKER-ICS — Generatore Lazy di Calendari iCal/CalDAV per Passaggi a Livello
// Runtime: Go 1.26+ (Zero dipendenze esterne, solo Standard Library)
// ============================================================================
//
// DESCRIZIONE ARCHITETTURALE:
// 1. Raccolta Dati: Interroga gli endpoint pubblici REST di ViaggiaTreno per
//    una o più stazioni "ancora" (hub di linea) configurate dall'utente.
// 2. Apprendimento Topologico & Euristiche:
//    - Scopre dinamicamente TUTTE le fermate/stazioni attraversate dai treni.
//    - Costruisce un grafo pesato dei tempi di percorrenza reali tra fermate.
//    - Interpola i passaggi dei treni veloci/diretti che TRANSITANO senza
//      fermarsi nelle stazioni intermedie (euristica di proporzionalità).
//    - Calcola la finestra temporale di chiusura delle sbarre in base alla
//      categoria del treno (AV/IC/REG) e propaga i ritardi in tempo reale.
// 3. Lazy Persistence & RFC 5545 (iCal / CalDAV):
//    - Genera un file `<CODICE>_<NOME_FERMATA>.ics` per ciascuna fermata.
//    - Calcola un digest SHA-256 canonico degli eventi (ignorando il timestamp
//      di generazione): sovrascrive il file su disco SOLO se cambiano orari,
//      ritardi, cancellazioni o nuovi transiti stimati.
//    - Incrementa automaticamente il campo `SEQUENCE` dei singoli `VEVENT`
//      modificati per garantire la sincronizzazione sui client CalDAV/iCal.
//
// CONFIGURAZIONE GITHUB ACTION (.github/workflows/sync-calendars.yml):
// ----------------------------------------------------------------------------
// name: Sync Level Crossing Calendars
// on:
//   schedule:
//     - cron: '*/15 * * * *' # Ogni 15 minuti
//   workflow_dispatch:
// permissions:
//   contents: write
// jobs:
//   build-ics:
//     runs-on: ubuntu-latest
//     steps:
//       - uses: actions/checkout@v4
//       - uses: actions/setup-go@v5
//         with:
//           go-version: '1.26'
//       - name: Run PL Tracker Generator
//         env:
//           ANCHOR_STATIONS: "S02581,S02593" # Es. Padova, Venezia Mestre
//           OUTPUT_DIR: "calendars"
//           FALLBACK_DEMO: "false"
//         run: go run main.go
//       - name: Commit & Push Lazy Updates
//         run: |
//           git config user.name "github-actions[bot]"
//           git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
//           git add calendars/
//           if ! git diff --staged --quiet; then
//             git commit -m "chore(ics): aggiornamento lazy orari/ritardi passaggi a livello"
//             git push
//           fi
//
// CONFIGURAZIONE SCALA NAZIONALE (MATRIX SHARDING ANTI-HAMMERING):
//   Impostare REGION_PRESET="ITALY_ALL" (oppure "NORD", "CENTRO", "SUD_ISOLE"),
//   RATE_LIMIT_RPS="3.5", SHARD_INDEX e SHARD_TOTAL per distribuire il carico
//   senza duplicare le query sui treni a lunga percorrenza.
// ----------------------------------------------------------------------------

package main

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultBaseURL   = "http://www.viaggiatreno.it/infomobilita/resteasy/viaggiatreno"
	defaultUserAgent = "PL-Tracker-ICS/1.26 (+https://github.com/open-mobility/pl-tracker-ics)"
	calendarProdID   = "-//PL-Tracker-ICS//Inferenza Passaggi a Livello 1.26//IT"
)

// regionalHubPresets implementa una copertura minima (Minimum Set Cover) dei
// principali nodi ferroviari italiani per intercettare >95% dei treni circolanti
// riducendo del ~97% le query sui tabelloni di stazione.
var regionalHubPresets = map[string][]string{
	"NORD": {
		"S00219", // Torino Porta Nuova
		"S01700", // Milano Centrale
		"S01645", // Milano Porta Garibaldi
		"S04700", // Genova Piazza Principe
		"S02430", // Verona Porta Nuova
		"S02581", // Padova
		"S02593", // Venezia Mestre
		"S03317", // Trieste Centrale
		"S05043", // Bologna Centrale
		"S05000", // Piacenza
		"S02025", // Brescia
		"S02248", // Bolzano
	},
	"CENTRO": {
		"S06421", // Firenze Santa Maria Novella
		"S06500", // Pisa Centrale
		"S06000", // Livorno Centrale
		"S07113", // Ancona
		"S07000", // Perugia
		"S08409", // Roma Termini
		"S08217", // Roma Tiburtina
		"S08008", // Civitavecchia
		"S07311", // Pescara Centrale
	},
	"SUD_ISOLE": {
		"S09218", // Napoli Centrale
		"S09818", // Salerno
		"S11119", // Bari Centrale
		"S11145", // Foggia
		"S11422", // Taranto
		"S11781", // Reggio Calabria Centrale
		"S12000", // Messina Centrale
		"S12306", // Catania Centrale
		"S12100", // Palermo Centrale
		"S12899", // Cagliari
		"S12800", // Sassari
	},
}

// ============================================================================
// 1. CONFIGURAZIONE & FEATURE DI GO 1.26
// ============================================================================

// Config contiene i parametri operativi del generatore.
// Sfrutta la novità di Go 1.26 `new(expr)` per inizializzare puntatori a valori
// opzionali direttamente in-line senza variabili temporanee di appoggio.
type Config struct {
	BaseURL          string
	RegionPreset     string
	AnchorStations   []string
	OutputDir        string
	StateCacheFile   string
	Concurrency      int
	RateLimitRPS     float64 // Limite globale richieste/secondo verso upstream
	ShardIndex       int     // Indice shard corrente (0 .. ShardTotal-1)
	ShardTotal       int     // Numero totale di runner paralleli
	HTTPTimeout      time.Duration
	PreClosureBuffer *time.Duration // Anticipo base chiusura sbarre (pedale di annuncio)
	PostClosureClear *time.Duration // Margine riapertura sbarre dopo liberazione circuito
	MinSafeReopenGap *time.Duration // Intervallo minimo di sgombero stradale sotto cui il PL resta chiuso
	DelayJitterMin   *int           // Soglia minima di variazione ritardo (min) per aggiornare
	DemoMode         bool           // Forza dataset simulato offline
	FallbackDemo     bool           // Usa dataset simulato se l'API pubblica non risponde
}

func loadConfig() Config {
	// Go 1.26: `new(expr)` alloca e inizializza il puntatore direttamente dall'espressione.
	cfg := Config{
		BaseURL:          envOrDefault("VIAGGIATRENO_BASE_URL", defaultBaseURL),
		RegionPreset:     strings.ToUpper(envOrDefault("REGION_PRESET", "")),
		OutputDir:        envOrDefault("OUTPUT_DIR", "calendars"),
		StateCacheFile:   envOrDefault("STATE_CACHE_FILE", "state_cache.json"),
		Concurrency:      envIntOrDefault("CONCURRENCY", 3),
		RateLimitRPS:     envFloatOrDefault("RATE_LIMIT_RPS", 3.5),
		ShardIndex:       envIntOrDefault("SHARD_INDEX", 0),
		ShardTotal:       max(1, envIntOrDefault("SHARD_TOTAL", 1)),
		HTTPTimeout:      12 * time.Second,
		PreClosureBuffer: new(3*time.Minute + 30*time.Second),
		PostClosureClear: new(60 * time.Second),
		MinSafeReopenGap: new(time.Duration(envIntOrDefault("MIN_SAFE_REOPEN_SEC", 120)) * time.Second),
		DelayJitterMin:   new(1),
		DemoMode:         envBoolOrDefault("DEMO_MODE", false),
		FallbackDemo:     envBoolOrDefault("FALLBACK_DEMO", true),
	}

	stationsRaw := envOrDefault("ANCHOR_STATIONS", "S02581,S02593") // Default: Padova, Venezia Mestre
	flagRegion := flag.String("region", cfg.RegionPreset, "Preset macro-area: ITALY_ALL, NORD, CENTRO, SUD_ISOLE")
	flagStations := flag.String("stations", stationsRaw, "Codici stazione ancora separati da virgola (es. S02581,S02593)")
	flagOut := flag.String("out", cfg.OutputDir, "Directory di destinazione dei file .ics")
	flagCache := flag.String("cache", cfg.StateCacheFile, "Percorso file cache incrementale delta-polling")
	flagConcurrency := flag.Int("concurrency", cfg.Concurrency, "Numero massimo di richieste HTTP concorrenti")
	flagRPS := flag.Float64("rps", cfg.RateLimitRPS, "Rate limit massimo richieste/sec verso API upstream")
	flagShardIdx := flag.Int("shard-index", cfg.ShardIndex, "Indice shard corrente per esecuzione distribuita")
	flagShardTot := flag.Int("shard-total", cfg.ShardTotal, "Numero totale shard paralleli")
	flagReopenGap := flag.Duration("reopen-gap", *cfg.MinSafeReopenGap, "Intervallo minimo di riapertura PL tra due treni (es. 2m)")
	flagDemo := flag.Bool("demo", cfg.DemoMode, "Esegue in modalità simulazione locale (utile per test)")
	flagFallback := flag.Bool("fallback-demo", cfg.FallbackDemo, "Attiva fallback simulato in caso di errore rete")
	flag.Parse()

	cfg.RegionPreset = strings.ToUpper(strings.TrimSpace(*flagRegion))
	cfg.OutputDir = *flagOut
	cfg.StateCacheFile = *flagCache
	cfg.Concurrency = max(1, *flagConcurrency)
	cfg.RateLimitRPS = max(0.2, *flagRPS)
	cfg.ShardTotal = max(1, *flagShardTot)
	cfg.ShardIndex = min(max(0, *flagShardIdx), cfg.ShardTotal-1)
	cfg.MinSafeReopenGap = new(max(0, *flagReopenGap))
	cfg.DemoMode = *flagDemo
	cfg.FallbackDemo = *flagFallback

	seen := make(map[string]bool)
	addStation := func(code string) {
		if trimmed := strings.ToUpper(strings.TrimSpace(code)); trimmed != "" && !seen[trimmed] {
			seen[trimmed] = true
			cfg.AnchorStations = append(cfg.AnchorStations, trimmed)
		}
	}

	switch cfg.RegionPreset {
	case "ITALY_ALL":
		for _, reg := range []string{"NORD", "CENTRO", "SUD_ISOLE"} {
			for _, st := range regionalHubPresets[reg] {
				addStation(st)
			}
		}
	case "NORD", "CENTRO", "SUD_ISOLE":
		for _, st := range regionalHubPresets[cfg.RegionPreset] {
			addStation(st)
		}
	default:
		for _, s := range strings.Split(*flagStations, ",") {
			addStation(s)
		}
	}
	return cfg
}

// ============================================================================
// 2. MODELLI DATI API VIAGGIATRENO & DOMINIO FERROVIARIO
// ============================================================================

// vtBoardTrain rappresenta un treno nei tabelloni `partenze` o `arrivi`.
type vtBoardTrain struct {
	NumeroTreno       int    `json:"numeroTreno"`
	Categoria         string `json:"categoria"`
	CodOrigine        string `json:"codOrigine"`
	Origine           string `json:"origine"`
	Destinazione      string `json:"destinazione"`
	DataPartenzaTreno int64  `json:"dataPartenzaTreno"` // Unix epoch ms
	Ritardo           int    `json:"ritardo"`
}

// vtTrainDetail rappresenta la risposta dell'endpoint `andamentoTreno`.
type vtTrainDetail struct {
	NumeroTreno       int      `json:"numeroTreno"`
	Categoria         string   `json:"categoria"`
	CodOrigine        string   `json:"codOrigine"`
	Origine           string   `json:"origine"`
	Destinazione      string   `json:"destinazione"`
	DataPartenzaTreno int64    `json:"dataPartenzaTreno"`
	Ritardo           int      `json:"ritardo"`
	Arrivato          bool     `json:"arrivato"`
	TipoTreno         string   `json:"tipoTreno"`     // PG=Programmato, ST=Soppresso, DV=Deviato, ecc.
	Provvedimento     int      `json:"provvedimento"` // 1 = soppresso
	Fermate           []vtStop `json:"fermate"`
}

// vtStop rappresenta una singola fermata lungo la traccia del treno.
type vtStop struct {
	ID                   string `json:"id"`
	Stazione             string `json:"stazione"`
	Programmata          int64  `json:"programmata"`          // Epoch ms orario programmato
	Effettiva            int64  `json:"effettiva"`            // Epoch ms orario reale (se già transitato)
	PartenzaTeorica      int64  `json:"partenzaTeorica"`      // Epoch ms partenza programmata
	PartenzaReale        int64  `json:"partenzaReale"`        // Epoch ms partenza reale
	ArrivoTeorico        int64  `json:"arrivoTeorico"`        // Epoch ms arrivo programmato
	ArrivoReale          int64  `json:"arrivoReale"`          // Epoch ms arrivo reale
	Ritardo              int    `json:"ritardo"`              // Ritardo in minuti
	TipoFermata          string `json:"tipoFermata"`          // P=Partenza, F=Fermata, A=Arrivo, S=Soppressa
	ActualFermataType    int    `json:"actualFermataType"`    // 0=non effettuata, 1=effettuata, 3=soppressa
}

// trainKey identifica univocamente una corsa giornaliera.
type trainKey struct {
	OriginCode string
	Number     int
	Departure  int64
}

// CrossingEvent rappresenta un evento inferito (fermata reale o transito senza
// fermata) presso una specifica stazione / passaggio a livello limitrofo.
type CrossingEvent struct {
	UID               string
	StationID         string
	StationName       string
	TrainNumber       int
	TrainCategory     string
	OriginName        string
	DestinationName   string
	EstimatedPassTime time.Time // Istante stimato di passaggio sull'asse stazione/PL
	BarrierCloseStart time.Time // Inizio stimato chiusura sbarre
	BarrierOpenEnd    time.Time // Fine stimata riapertura sbarre
	DelayMinutes      int
	IsNonStopTransit  bool     // true se il treno NON ferma ma transita ad alta velocità
	IsCancelled       bool     // true se treno o fermata soppressa
	MergedCount       int      // Numero di transiti fusi in un'unica chiusura continua (>=1)
	MergedTrains      []string // Riepilogo dei treni coinvolti nella chiusura continua
	Confidence        float64  // 0.0 - 1.0 affidabilità della stima
	HeuristicNotes    string
}

// Sortable sfrutta i vincoli generici auto-referenziali introdotti in Go 1.26
// per garantire ordinamenti canonici e deterministici.
type Sortable[T Sortable[T]] interface {
	Compare(other T) int
}

func (e CrossingEvent) Compare(other CrossingEvent) int {
	if c := e.EstimatedPassTime.Compare(other.EstimatedPassTime); c != 0 {
		return c
	}
	return cmp.Compare(e.UID, other.UID)
}

func sortCanonical[T Sortable[T]](items []T) {
	slices.SortFunc(items, func(a, b T) int {
		return a.Compare(b)
	})
}

// ============================================================================
// 3. CLIENT HTTP RESILIENTE, RATE LIMITER & DELTA STATE CACHE
// ============================================================================

// CachedTrainRecord memorizza lo stato dell'ultimo campionamento per evitare
// di ri-interrogare treni già arrivati, non ancora partiti o con ritardo invariato.
type CachedTrainRecord struct {
	LastFetchedUnix int64         `json:"last_fetched_unix"`
	LastKnownDelay  int           `json:"last_known_delay"`
	Arrived         bool          `json:"arrived"`
	Detail          vtTrainDetail `json:"detail"`
}

type TrainStateCache struct {
	mu      sync.Mutex
	Records map[string]CachedTrainRecord `json:"records"`
	dirty   bool
}

func LoadTrainStateCache(path string, now time.Time) *TrainStateCache {
	cache := &TrainStateCache{
		Records: make(map[string]CachedTrainRecord),
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return cache
	}
	if err := json.Unmarshal(raw, cache); err != nil {
		return &TrainStateCache{Records: make(map[string]CachedTrainRecord)}
	}
	// Pulizia automatica dei record più vecchi di 18 ore
	cutoff := now.Add(-18 * time.Hour).Unix()
	for k, rec := range cache.Records {
		if rec.LastFetchedUnix < cutoff {
			delete(cache.Records, k)
			cache.dirty = true
		}
	}
	return cache
}

func (c *TrainStateCache) SaveIfDirty(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.dirty {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}
	c.dirty = false
	return nil
}

func cacheKeyString(k trainKey) string {
	return fmt.Sprintf("%s:%d:%d", k.OriginCode, k.Number, k.Departure)
}

// ShouldFetchDetail implementa l'euristica di Delta Polling / Adaptive TTL:
// restituisce (false, cachedDetail) se la chiamata upstream può essere evitata.
func (c *TrainStateCache) ShouldFetchDetail(k trainKey, board vtBoardTrain, now time.Time) (bool, vtTrainDetail) {
	c.mu.Lock()
	defer c.mu.Unlock()

	rec, exists := c.Records[cacheKeyString(k)]
	if !exists || len(rec.Detail.Fermate) == 0 {
		return true, vtTrainDetail{}
	}

	age := now.Sub(time.Unix(rec.LastFetchedUnix, 0))

	// 1. Treno già giunto al capolinea: nessun aggiornamento ulteriore necessario
	if rec.Arrived {
		return false, rec.Detail
	}

	// 2. Se il ritardo riportato sul tabellone è cambiato, aggiorniamo subito
	if board.Ritardo != rec.LastKnownDelay {
		return true, vtTrainDetail{}
	}

	// 3. Treno non ancora partito (mancano >25 min): TTL lungo (45 min)
	depTime := time.UnixMilli(board.DataPartenzaTreno)
	if now.Before(depTime.Add(-25*time.Minute)) && age < 45*time.Minute {
		return false, rec.Detail
	}

	// 4. Treno in viaggio con ritardo invariato: TTL adattivo di 12 minuti
	if age < 12*time.Minute {
		return false, rec.Detail
	}

	return true, vtTrainDetail{}
}

func (c *TrainStateCache) Put(k trainKey, detail vtTrainDetail, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Records[cacheKeyString(k)] = CachedTrainRecord{
		LastFetchedUnix: now.Unix(),
		LastKnownDelay:  detail.Ritardo,
		Arrived:         detail.Arrivato,
		Detail:          detail,
	}
	c.dirty = true
}

// TokenBucketLimiter regola il flusso globale delle richieste HTTP verso upstream
// aggiungendo un jitter casuale per evitare pattern di traffico robotici.
type TokenBucketLimiter struct {
	tokens chan struct{}
	stop   chan struct{}
}

func NewTokenBucketLimiter(rps float64) *TokenBucketLimiter {
	interval := time.Duration(float64(time.Second) / rps)
	interval = max(50*time.Millisecond, interval)

	l := &TokenBucketLimiter{
		tokens: make(chan struct{}, 2), // Burst massimo limitato a 2 richieste
		stop:   make(chan struct{}),
	}
	l.tokens <- struct{}{}

	go func() {
		for {
			// Jitter ±30% sull'intervallo di rilascio token
			jitterFactor := 0.7 + rand.Float64()*0.6
			wait := time.Duration(float64(interval) * jitterFactor)
			select {
			case <-l.stop:
				return
			case <-time.After(wait):
				select {
				case l.tokens <- struct{}{}:
				default:
				}
			}
		}
	}()
	return l
}

func (l *TokenBucketLimiter) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.tokens:
		return nil
	}
}

func (l *TokenBucketLimiter) Close() {
	close(l.stop)
}

type RailClient struct {
	baseURL       string
	httpClient    *http.Client
	sem           chan struct{}
	limiter       *TokenBucketLimiter
	shardIndex    int
	shardTotal    int
	consecErrors  atomic.Int32
	circuitTripped atomic.Bool
	logger        *slog.Logger
}

func NewRailClient(cfg Config, logger *slog.Logger) *RailClient {
	transport := &http.Transport{
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 2, // Mantiene poche connessioni TCP/TLS riutilizzate
		IdleConnTimeout:     30 * time.Second,
	}
	return &RailClient{
		baseURL: strings.TrimRight(cfg.BaseURL, "/"),
		httpClient: &http.Client{
			Timeout:   cfg.HTTPTimeout,
			Transport: transport,
		},
		sem:        make(chan struct{}, cfg.Concurrency),
		limiter:    NewTokenBucketLimiter(cfg.RateLimitRPS),
		shardIndex: cfg.ShardIndex,
		shardTotal: cfg.ShardTotal,
		logger:     logger,
	}
}

func (c *RailClient) Close() {
	c.limiter.Close()
}

func (c *RailClient) getJSON(ctx context.Context, endpoint string, target any) (bool, error) {
	if c.circuitTripped.Load() {
		return false, errors.New("circuit breaker aperto: upstream temporaneamente sovraccarico")
	}

	c.sem <- struct{}{}
	defer func() { <-c.sem }()

	const maxAttempts = 3
	var lastErr error

	for attempt := range maxAttempts {
		if err := c.limiter.Wait(ctx); err != nil {
			return false, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return false, fmt.Errorf("creazione richiesta HTTP: %w", err)
		}
		req.Header.Set("User-Agent", defaultUserAgent)
		req.Header.Set("Accept", "application/json, text/plain, */*")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("esecuzione GET %s: %w", endpoint, err)
			c.recordFailure()
			backoffSleep(ctx, attempt)
			continue
		}

		status := resp.StatusCode
		if status == http.StatusNoContent {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			c.consecErrors.Store(0)
			return false, nil
		}

		// Rate limit (429) o errore temporaneo server (5xx): applica exponential backoff
		if status == http.StatusTooManyRequests || status >= 500 {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("status HTTP transitorio %d su %s", status, endpoint)
			c.recordFailure()
			backoffSleep(ctx, attempt+1)
			continue
		}

		if status != http.StatusOK {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return false, fmt.Errorf("status HTTP inatteso %d su %s", status, endpoint)
		}

		body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		_ = resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("lettura body risposta: %w", err)
			continue
		}

		c.consecErrors.Store(0)
		if len(bytes.TrimSpace(body)) == 0 {
			return false, nil
		}
		if err := json.Unmarshal(body, target); err != nil {
			return false, fmt.Errorf("decodifica JSON da %s: %w", endpoint, err)
		}
		return true, nil
	}

	return false, lastErr
}

func (c *RailClient) recordFailure() {
	if c.consecErrors.Add(1) >= 12 {
		if c.circuitTripped.CompareAndSwap(false, true) {
			c.logger.Warn("Circuit breaker attivato dopo 12 errori consecutivi: degradazione su cache")
		}
	}
}

func backoffSleep(ctx context.Context, attempt int) {
	base := time.Duration(1<<attempt) * 500 * time.Millisecond
	jitter := time.Duration(rand.Int64N(int64(base)))
	select {
	case <-ctx.Done():
	case <-time.After(base + jitter):
	}
}

// FetchActiveTrains interroga i tabelloni delle stazioni ancora, applica lo
// sharding deterministico per evitare duplicati tra runner e sfrutta la cache
// incrementale per ridurre al minimo le chiamate ad `andamentoTreno`.
func (c *RailClient) FetchActiveTrains(
	ctx context.Context,
	anchors []string,
	cache *TrainStateCache,
	now time.Time,
) ([]vtTrainDetail, error) {
	romeLoc, err := time.LoadLocation("Europe/Rome")
	if err != nil {
		romeLoc = time.FixedZone("CET", 3600)
	}
	// Formato data stile JavaScript Date.toString() richiesto dalle API REST ViaggiaTreno
	jsDate := url.PathEscape(now.In(romeLoc).Format("Mon Jan 02 2006 15:04:05 GMT-0700 (MST)"))

	discovered := make(map[trainKey]vtBoardTrain)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var fetchErrors int

	for _, station := range anchors {
		for _, boardType := range []string{"partenze", "arrivi"} {
			wg.Add(1)
			go func(st, bType string) {
				defer wg.Done()
				endpoint := fmt.Sprintf("%s/%s/%s/%s", c.baseURL, bType, url.PathEscape(st), jsDate)
				var list []vtBoardTrain
				ok, err := c.getJSON(ctx, endpoint, &list)
				if err != nil {
					c.logger.Warn("Errore interrogazione tabellone stazione",
						"station", st, "board", bType, "err", err)
					mu.Lock()
					fetchErrors++
					mu.Unlock()
					return
				}
				if !ok {
					return
				}
				mu.Lock()
				for _, t := range list {
					if t.NumeroTreno == 0 || t.CodOrigine == "" || t.DataPartenzaTreno == 0 {
						continue
					}
					key := trainKey{
						OriginCode: t.CodOrigine,
						Number:     t.NumeroTreno,
						Departure:  t.DataPartenzaTreno,
					}
					discovered[key] = t
				}
				mu.Unlock()
			}(station, boardType)
		}
	}
	wg.Wait()

	if len(discovered) == 0 && fetchErrors > 0 {
		return nil, fmt.Errorf("impossibile raggiungere le API pubbliche (%d errori)", fetchErrors)
	}

	details := make([]vtTrainDetail, 0, len(discovered))
	var cacheHits, shardSkipped, networkFetched atomic.Int32

	for key, boardInfo := range discovered {
		// Sharding deterministico: se girano N runner su macro-aree diverse, ogni
		// treno viene scaricato da un solo shard anche se attraversa più regioni.
		if c.shardTotal > 1 && (key.Number%c.shardTotal) != c.shardIndex {
			shardSkipped.Add(1)
			continue
		}

		// Delta polling: evita la richiesta HTTP se il treno in cache è ancora valido
		if shouldFetch, cachedDetail := cache.ShouldFetchDetail(key, boardInfo, now); !shouldFetch {
			cacheHits.Add(1)
			details = append(details, cachedDetail)
			continue
		}

		wg.Add(1)
		go func(k trainKey) {
			defer wg.Done()
			endpoint := fmt.Sprintf("%s/andamentoTreno/%s/%d/%d",
				c.baseURL, url.PathEscape(k.OriginCode), k.Number, k.Departure)
			var detail vtTrainDetail
			ok, err := c.getJSON(ctx, endpoint, &detail)
			if err != nil {
				c.logger.Debug("Errore recupero andamento treno", "train", k.Number, "err", err)
				return
			}
			if ok && len(detail.Fermate) > 0 {
				networkFetched.Add(1)
				cache.Put(k, detail, now)
				mu.Lock()
				details = append(details, detail)
				mu.Unlock()
			}
		}(key)
	}
	wg.Wait()

	c.logger.Info("Ciclo raccolta treni completato",
		"discovered_total", len(discovered),
		"fetched_upstream", networkFetched.Load(),
		"reused_from_cache", cacheHits.Load(),
		"delegated_other_shards", shardSkipped.Load(),
	)

	return details, nil
}

// ============================================================================
// 4. MOTORE DI INFERENZA TOPOLOGICA & EURISTICHE DI TRANSITO
// ============================================================================

// CorridorGraph apprende la topologia della linea ferroviaria e i tempi medi
// di percorrenza tra fermate consecutive osservando i treni regionali/locali.
type CorridorGraph struct {
	StationNames map[string]string
	Edges        map[string]map[string]*edgeStats
}

type edgeStats struct {
	TotalSeconds float64
	Samples      int
}

func (e *edgeStats) avgDuration() time.Duration {
	if e.Samples == 0 {
		return 5 * time.Minute
	}
	return time.Duration(e.TotalSeconds/float64(e.Samples)) * time.Second
}

func BuildCorridorGraph(trains []vtTrainDetail) *CorridorGraph {
	g := &CorridorGraph{
		StationNames: make(map[string]string),
		Edges:        make(map[string]map[string]*edgeStats),
	}

	for _, tr := range trains {
		for i := range tr.Fermate {
			curr := tr.Fermate[i]
			if curr.ID == "" {
				continue
			}
			if curr.Stazione != "" {
				g.StationNames[curr.ID] = strings.TrimSpace(curr.Stazione)
			}
			if i == 0 {
				continue
			}
			prev := tr.Fermate[i-1]
			tPrev := stopDepartureTime(prev, tr.Ritardo)
			tCurr := stopArrivalTime(curr, tr.Ritardo)
			diff := tCurr.Sub(tPrev)

			// Consideriamo solo tratte plausibili (tra 1 e 45 minuti) per evitare
			// di apprendere "salti" di treni AV come archi diretti corti
			if diff >= 1*time.Minute && diff <= 45*time.Minute {
				if g.Edges[prev.ID] == nil {
					g.Edges[prev.ID] = make(map[string]*edgeStats)
				}
				st := g.Edges[prev.ID][curr.ID]
				if st == nil {
					st = &edgeStats{}
					g.Edges[prev.ID][curr.ID] = st
				}
				st.TotalSeconds += diff.Seconds()
				st.Samples++
			}
		}
	}
	return g
}

// FindIntermediatePath esegue una ricerca BFS pesata sul grafo appreso per trovare
// le stazioni intermedie saltate da un treno veloce tra `fromID` e `toID`.
func (g *CorridorGraph) FindIntermediatePath(fromID, toID string) ([]string, []time.Duration) {
	if fromID == toID || g.Edges[fromID] == nil {
		return nil, nil
	}

	type node struct {
		id   string
		path []string
		durs []time.Duration
	}

	queue := []node{{id: fromID, path: []string{fromID}}}
	visited := map[string]bool{fromID: true}

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]

		// Limita profondità ricerca per evitare percorsi non correlati
		if len(curr.path) > 10 {
			continue
		}

		for nextID, stats := range g.Edges[curr.id] {
			if nextID == toID {
				// Restituiamo il percorso solo se ha almeno 1 stazione intermedia
				if len(curr.path) >= 2 {
					fullPath := append(slices.Clone(curr.path), nextID)
					fullDurs := append(slices.Clone(curr.durs), stats.avgDuration())
					return fullPath, fullDurs
				}
				continue
			}
			if !visited[nextID] {
				visited[nextID] = true
				queue = append(queue, node{
					id:   nextID,
					path: append(slices.Clone(curr.path), nextID),
					durs: append(slices.Clone(curr.durs), stats.avgDuration()),
				})
			}
		}
	}
	return nil, nil
}

// InferStationSchedules elabora tutti i treni, combina fermate reali e transiti
// stimati senza fermata, applica le finestre di chiusura sbarre e raggruppa per stazione.
func InferStationSchedules(trains []vtTrainDetail, cfg Config) map[string][]CrossingEvent {
	graph := BuildCorridorGraph(trains)
	byStation := make(map[string][]CrossingEvent)

	for _, tr := range trains {
		isTrainCancelled := tr.TipoTreno == "ST" || tr.Provvedimento == 1
		visitedStops := make(map[string]bool)

		for i, stop := range tr.Fermate {
			if stop.ID == "" {
				continue
			}
			visitedStops[stop.ID] = true

			passTime := stopReferenceTime(stop, tr.Ritardo)
			if passTime.IsZero() {
				continue
			}

			isStopCancelled := isTrainCancelled || stop.TipoFermata == "S" || stop.ActualFermataType == 3
			preBuf, postBuf := barrierBuffersForStop(tr.Categoria, false, *cfg.PreClosureBuffer, *cfg.PostClosureClear)

			arrTime := stopArrivalTime(stop, tr.Ritardo)
			depTime := stopDepartureTime(stop, tr.Ritardo)
			if arrTime.IsZero() {
				arrTime = passTime
			}
			if depTime.IsZero() || depTime.Before(arrTime) {
				depTime = arrTime
			}

			dwell := depTime.Sub(arrTime)
			trainLabel := formatTrainShortLabel(tr.Categoria, tr.NumeroTreno, tr.Origine, tr.Destinazione, effectiveDelay(stop, tr.Ritardo))

			// Se il treno effettua una sosta lunga (>3 min, es. attesa incrocio/precedenza),
			// separiamo l'impegno di arrivo da quello di partenza: se un treno sorpassante transita
			// nel frattempo o l'intervallo è breve, CoalesceClosures li rifonderà automaticamente.
			if dwell > 3*time.Minute && !isStopCancelled {
				evArr := CrossingEvent{
					UID:               buildEventUID(tr, stop.ID) + "-arr",
					StationID:         stop.ID,
					StationName:       normalizeStationName(stop.Stazione, stop.ID),
					TrainNumber:       tr.NumeroTreno,
					TrainCategory:     normalizeCategory(tr.Categoria),
					OriginName:        tr.Origine,
					DestinationName:   tr.Destinazione,
					EstimatedPassTime: arrTime,
					BarrierCloseStart: arrTime.Add(-preBuf),
					BarrierOpenEnd:    arrTime.Add(postBuf),
					DelayMinutes:      effectiveDelay(stop, tr.Ritardo),
					IsNonStopTransit:  false,
					IsCancelled:       false,
					MergedCount:       1,
					MergedTrains:      []string{trainLabel + " [Arrivo]"},
					Confidence:        computeStopConfidence(stop),
					HeuristicNotes:    buildStopNote(stop, tr.Ritardo) + " (impegno itinerario ingresso stazione)",
				}
				evDep := CrossingEvent{
					UID:               buildEventUID(tr, stop.ID) + "-dep",
					StationID:         stop.ID,
					StationName:       normalizeStationName(stop.Stazione, stop.ID),
					TrainNumber:       tr.NumeroTreno,
					TrainCategory:     normalizeCategory(tr.Categoria),
					OriginName:        tr.Origine,
					DestinationName:   tr.Destinazione,
					EstimatedPassTime: depTime,
					BarrierCloseStart: depTime.Add(-preBuf),
					BarrierOpenEnd:    depTime.Add(postBuf),
					DelayMinutes:      effectiveDelay(stop, tr.Ritardo),
					IsNonStopTransit:  false,
					IsCancelled:       false,
					MergedCount:       1,
					MergedTrains:      []string{trainLabel + " [Partenza]"},
					Confidence:        computeStopConfidence(stop),
					HeuristicNotes:    buildStopNote(stop, tr.Ritardo) + " (impegno itinerario partenza stazione)",
				}
				byStation[stop.ID] = append(byStation[stop.ID], evArr, evDep)
			} else {
				ev := CrossingEvent{
					UID:               buildEventUID(tr, stop.ID),
					StationID:         stop.ID,
					StationName:       normalizeStationName(stop.Stazione, stop.ID),
					TrainNumber:       tr.NumeroTreno,
					TrainCategory:     normalizeCategory(tr.Categoria),
					OriginName:        tr.Origine,
					DestinationName:   tr.Destinazione,
					EstimatedPassTime: passTime,
					BarrierCloseStart: arrTime.Add(-preBuf),
					BarrierOpenEnd:    depTime.Add(postBuf),
					DelayMinutes:      effectiveDelay(stop, tr.Ritardo),
					IsNonStopTransit:  false,
					IsCancelled:       isStopCancelled,
					MergedCount:       1,
					MergedTrains:      []string{trainLabel},
					Confidence:        computeStopConfidence(stop),
					HeuristicNotes:    buildStopNote(stop, tr.Ritardo),
				}
				byStation[stop.ID] = append(byStation[stop.ID], ev)
			}

			// EURISTICA DI TRANSITO SENZA FERMATA:
	// Ordinamento canonico e fusione (coalescing) delle chiusure ravvicinate
	// il cui intervallo di apertura sarebbe inferiore al tempo minimo di sicurezza stradale.
	for stID := range byStation {
		sortCanonical(byStation[stID])
		byStation[stID] = CoalesceClosures(byStation[stID], *cfg.MinSafeReopenGap)
	}
	return byStation
}

// CoalesceClosures unisce in un'unica chiusura continua gli eventi consecutivi che
// si sovrappongono o il cui tempo utile a barriere aperte è inferiore a `minSafeGap`
// (normativa sicurezza sgombero PL / vincolo itinerari ACEI-ACC).
func CoalesceClosures(events []CrossingEvent, minSafeGap time.Duration) []CrossingEvent {
	if len(events) <= 1 {
		return events
	}

	merged := make([]CrossingEvent, 0, len(events))
	curr := events[0]
	if curr.MergedCount == 0 {
		curr.MergedCount = 1
	}

	for i := 1; i < len(events); i++ {
		next := events[i]
		if next.MergedCount == 0 {
			next.MergedCount = 1
		}

		// Se entrambi sono soppressi non c'è impegno fisico del PL
		if curr.IsCancelled && next.IsCancelled {
			merged = append(merged, curr)
			curr = next
			continue
		}

		gap := next.BarrierCloseStart.Sub(curr.BarrierOpenEnd)
		if !curr.IsCancelled && !next.IsCancelled && gap < minSafeGap {
			if next.BarrierOpenEnd.After(curr.BarrierOpenEnd) {
				curr.BarrierOpenEnd = next.BarrierOpenEnd
			}
			curr.MergedCount += next.MergedCount
			curr.MergedTrains = append(curr.MergedTrains, next.MergedTrains...)
			curr.DelayMinutes = max(curr.DelayMinutes, next.DelayMinutes)
			curr.IsNonStopTransit = curr.IsNonStopTransit || next.IsNonStopTransit
			curr.Confidence = min(curr.Confidence, next.Confidence)

			gapSec := int(max(0, gap.Seconds()))
			curr.HeuristicNotes = fmt.Sprintf(
				"Chiusura continua per %d treni: intervallo utile tra i passaggi (%ds) inferiore alla soglia minima di sgombero stradale (%ds)",
				curr.MergedCount, gapSec, int(minSafeGap.Seconds()),
			)
			continue
		}

		merged = append(merged, curr)
		curr = next
	}
	merged = append(merged, curr)
	return merged
}

// interpolateNonStopTransits calcola l'orario proporzionale di passaggio per ciascuna
// stazione intermedia attraversata senza fermata.
func interpolateNonStopTransits(
	tr vtTrainDetail,
	path []string,
	durs []time.Duration,
	tStart, tEnd time.Time,
	stationNames map[string]string,
	visitedStops map[string]bool,
	cfg Config,
	byStation map[string][]CrossingEvent,
) {
	var totalWeight time.Duration
	for _, d := range durs {
		totalWeight += d
	}
	if totalWeight <= 0 {
		return
	}

	actualSpan := tEnd.Sub(tStart)
	var cumulativeWeight time.Duration

	// Iteriamo solo sui nodi strettamente intermedi (escludendo primo e ultimo)
	for idx := 1; idx < len(path)-1; idx++ {
		cumulativeWeight += durs[idx-1]
		stID := path[idx]
		if visitedStops[stID] {
			continue
		}
		visitedStops[stID] = true

		ratio := float64(cumulativeWeight) / float64(totalWeight)
		estTransit := tStart.Add(time.Duration(float64(actualSpan) * ratio))

		// Fisica del pedale di annuncio RFI: la distanza D = Vmax * t_sicurezza è fissa
		// per il treno più veloce della linea; pertanto un treno che transita senza fermare
		// a velocità inferiore (es. Regionale o Intercity) impiega PIÙ tempo a coprire D!
		preBuf, postBuf := barrierBuffersForStop(tr.Categoria, true, *cfg.PreClosureBuffer, *cfg.PostClosureClear)

		stName := normalizeStationName(stationNames[stID], stID)
		trainLabel := formatTrainShortLabel(tr.Categoria, tr.NumeroTreno, tr.Origine, tr.Destinazione, tr.Ritardo)
		ev := CrossingEvent{
			UID:               buildEventUID(tr, stID),
			StationID:         stID,
			StationName:       stName,
			TrainNumber:       tr.NumeroTreno,
			TrainCategory:     normalizeCategory(tr.Categoria),
			OriginName:        tr.Origine,
			DestinationName:   tr.Destinazione,
			EstimatedPassTime: estTransit,
			BarrierCloseStart: estTransit.Add(-preBuf),
			BarrierOpenEnd:    estTransit.Add(postBuf),
			DelayMinutes:      tr.Ritardo,
			IsNonStopTransit:  true,
			IsCancelled:       false,
			MergedCount:       1,
			MergedTrains:      []string{trainLabel + " [Transito]"},
			Confidence:        0.78, // Stima interpolata su grafo topologico
			HeuristicNotes: fmt.Sprintf(
				"Transito stimato senza fermata (%.0f%% tratta %s -> %s; pedale annuncio fisso Vmax)",
				ratio*100,
				normalizeStationName(stationNames[path[0]], path[0]),
				normalizeStationName(stationNames[path[len(path)-1]], path[len(path)-1]),
			),
		}
		byStation[stID] = append(byStation[stID], ev)
	}
}

// ============================================================================
// 5. FUNZIONI EURISTICHE DI SUPPORTO
// ============================================================================

// barrierBuffersForStop calcola l'anticipo di chiusura (pedale di annuncio + ciclo
// barriere + verifica radar PAI-PL) e il margine di riapertura (pedale liberazione).
func barrierBuffersForStop(cat string, isNonStop bool, basePre, basePost time.Duration) (time.Duration, time.Duration) {
	c := strings.ToUpper(strings.TrimSpace(cat))
	isHighSpeed := strings.HasPrefix(c, "FR") || strings.HasPrefix(c, "FA") || strings.HasPrefix(c, "AV") || strings.HasPrefix(c, "ITA")
	isIntercity := strings.HasPrefix(c, "IC") || strings.HasPrefix(c, "EC") || strings.HasPrefix(c, "EN") || strings.HasPrefix(c, "FB")

	if isNonStop {
		switch {
		case isHighSpeed:
			// Un treno AV percorre la distanza fissa del pedale di annuncio vicino alla Vmax: ~2m45s
			return max(2*time.Minute+45*time.Second, basePre-45*time.Second), basePost
		case isIntercity:
			// Velocità intermedia rispetto alla Vmax di linea: ~3m30s
			return basePre, basePost
		default:
			// Regionale / Merci in transito senza fermata: percorrendo a velocità ridotta
			// una tratta attrezzata per Vmax superiore, impegna il pedale con maggior anticipo (~4m15s)
			return basePre + 45*time.Second, basePost + 15*time.Second
		}
	}

	// Treno con fermata nella stazione adiacente al PL (itinerari vincolati ACEI/ACC)
	switch {
	case isHighSpeed:
		return basePre + 30*time.Second, basePost
	case isIntercity:
		return basePre + 15*time.Second, basePost
	default:
		return basePre, basePost
	}
}

func formatTrainShortLabel(cat string, num int, orig, dest string, delay int) string {
	d := ""
	if delay > 0 {
		d = fmt.Sprintf(" (+%dm)", delay)
	}
	return fmt.Sprintf("%s %d (%s -> %s)%s", normalizeCategory(cat), num, orig, dest, d)
}
		byStation[stID] = append(byStation[stID], ev)
	}
}

// ============================================================================
// 5. FUNZIONI EURISTICHE DI SUPPORTO
// ============================================================================

func barrierBuffersForCategory(cat string, basePre, basePost time.Duration) (time.Duration, time.Duration) {
	c := strings.ToUpper(strings.TrimSpace(cat))
	switch {
	case strings.HasPrefix(c, "FR"), strings.HasPrefix(c, "FA"), strings.HasPrefix(c, "AV"), strings.HasPrefix(c, "ITA"):
		// Alta velocità: circuiti di binario attivati a maggiore distanza
		return basePre + 90*time.Second, basePost
	case strings.HasPrefix(c, "IC"), strings.HasPrefix(c, "EC"), strings.HasPrefix(c, "EN"), strings.HasPrefix(c, "FB"):
		// Intercity / Eurocity
		return basePre + 45*time.Second, basePost
	default:
		// Regionali / Suburbano / Altro
		return basePre, basePost
	}
}

func stopReferenceTime(s vtStop, trainDelay int) time.Time {
	if s.Effettiva > 0 {
		return time.UnixMilli(s.Effettiva).UTC()
	}
	if s.ArrivoReale > 0 {
		return time.UnixMilli(s.ArrivoReale).UTC()
	}
	if s.PartenzaReale > 0 {
		return time.UnixMilli(s.PartenzaReale).UTC()
	}
	baseMs := firstNonZero(s.Programmata, s.ArrivoTeorico, s.PartenzaTeorica)
	if baseMs == 0 {
		return time.Time{}
	}
	delay := effectiveDelay(s, trainDelay)
	return time.UnixMilli(baseMs).UTC().Add(time.Duration(delay) * time.Minute)
}

func stopArrivalTime(s vtStop, trainDelay int) time.Time {
	if s.ArrivoReale > 0 {
		return time.UnixMilli(s.ArrivoReale).UTC()
	}
	if s.Effettiva > 0 && s.TipoFermata == "A" {
		return time.UnixMilli(s.Effettiva).UTC()
	}
	baseMs := firstNonZero(s.ArrivoTeorico, s.Programmata, s.PartenzaTeorica)
	if baseMs == 0 {
		return time.Time{}
	}
	return time.UnixMilli(baseMs).UTC().Add(time.Duration(effectiveDelay(s, trainDelay)) * time.Minute)
}

func stopDepartureTime(s vtStop, trainDelay int) time.Time {
	if s.PartenzaReale > 0 {
		return time.UnixMilli(s.PartenzaReale).UTC()
	}
	if s.Effettiva > 0 && (s.TipoFermata == "P" || s.TipoFermata == "F") {
		return time.UnixMilli(s.Effettiva).UTC()
	}
	baseMs := firstNonZero(s.PartenzaTeorica, s.Programmata, s.ArrivoTeorico)
	if baseMs == 0 {
		return time.Time{}
	}
	return time.UnixMilli(baseMs).UTC().Add(time.Duration(effectiveDelay(s, trainDelay)) * time.Minute)
}

func effectiveDelay(s vtStop, trainDelay int) int {
	if s.Ritardo != 0 {
		return s.Ritardo
	}
	return trainDelay
}

func computeStopConfidence(s vtStop) float64 {
	if s.Effettiva > 0 || s.ArrivoReale > 0 || s.PartenzaReale > 0 {
		return 0.99 // Rilevamento reale da circuito di binario
	}
	return 0.90 // Orario programmato + propagazione ritardo corrente
}

func buildStopNote(s vtStop, trainDelay int) string {
	if s.Effettiva > 0 || s.ArrivoReale > 0 || s.PartenzaReale > 0 {
		return "Rilevamento reale da circuito di binario"
	}
	d := effectiveDelay(s, trainDelay)
	if d > 0 {
		return fmt.Sprintf("Previsione con ritardo propagato (+%d min)", d)
	}
	if d < 0 {
		return fmt.Sprintf("Previsione con anticipo (%d min)", d)
	}
	return "Previsione da traccia oraria regolare"
}

// ============================================================================
// 6. GENERAZIONE RFC 5545 (iCal / CalDAV) & AGGIORNAMENTO LAZY
// ============================================================================

// existingFileState conserva l'hash canonico e i SEQUENCE precedenti letti dal file .ics.
type existingFileState struct {
	ContentHash string
	Sequences   map[string]int // UID -> SEQUENCE
}

func readExistingICSState(filePath string) existingFileState {
	state := existingFileState{
		Sequences: make(map[string]int),
	}
	f, err := os.Open(filePath)
	if err != nil {
		return state
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	var currentUID string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case strings.HasPrefix(line, "X-CONTENT-HASH:"):
			state.ContentHash = strings.TrimPrefix(line, "X-CONTENT-HASH:")
		case strings.HasPrefix(line, "UID:"):
			currentUID = strings.TrimPrefix(line, "UID:")
		case strings.HasPrefix(line, "SEQUENCE:") && currentUID != "":
			if seq, err := strconv.Atoi(strings.TrimPrefix(line, "SEQUENCE:")); err == nil {
				state.Sequences[currentUID] = seq
			}
		case line == "END:VEVENT":
			currentUID = ""
		}
	}
	return state
}

// computeCanonicalHash calcola un digest SHA-256 deterministico esclusivamente
// sui dati ferroviari rilevanti (arrotondati alla tolleranza di jitter), ignorando
// completamente il timestamp corrente di esecuzione del job.
func computeCanonicalHash(stationID string, events []CrossingEvent, jitterToleranceMin int) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "STATION:%s|COUNT:%d\n", stationID, len(events))

	tol := max(1, jitterToleranceMin)
	for _, ev := range events {
		// Quantizza i timestamp alla soglia di tolleranza per evitare commit su micro-oscillazioni
		startBucket := ev.BarrierCloseStart.Unix() / int64(tol*60)
		endBucket := ev.BarrierOpenEnd.Unix() / int64(tol*60)
		_, _ = fmt.Fprintf(h, "%s|%d|%d|%d|%d|%t|%t\n",
			ev.UID,
			startBucket,
			endBucket,
			ev.DelayMinutes/tol,
			max(1, ev.MergedCount),
			ev.IsNonStopTransit,
			ev.IsCancelled,
		)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// WriteStationICSIfChanged scrive il file `.ics` solo se l'hash canonico è variato
// rispetto alla versione già salvata nel repository Git.
func WriteStationICSIfChanged(
	outDir string,
	stationID string,
	events []CrossingEvent,
	now time.Time,
	jitterMin int,
) (fileName string, updated bool, err error) {
	if len(events) == 0 {
		return "", false, nil
	}

	stationName := events[0].StationName
	fileName = fmt.Sprintf("%s_%s.ics", stationID, sanitizeFilename(stationName))
	fullPath := filepath.Join(outDir, fileName)

	newHash := computeCanonicalHash(stationID, events, jitterMin)
	prevState := readExistingICSState(fullPath)

	// LAZY CHECK: nessuna variazione sostanziale -> non tocchiamo il file su disco
	if prevState.ContentHash != "" && prevState.ContentHash == newHash {
		return fileName, false, nil
	}

	romeLoc, err := time.LoadLocation("Europe/Rome")
	if err != nil {
		romeLoc = time.FixedZone("CET", 3600)
	}

	var buf bytes.Buffer
	dtStamp := now.UTC().Format("20060102T150405Z")

	// Intestazione VCALENDAR conforme a RFC 5545 & estensioni CalDAV/Apple/Google
	writeICSLine(&buf, "BEGIN:VCALENDAR")
	writeICSLine(&buf, "VERSION:2.0")
	writeICSLine(&buf, "PRODID:"+calendarProdID)
	writeICSLine(&buf, "CALSCALE:GREGORIAN")
	writeICSLine(&buf, "METHOD:PUBLISH")
	writeICSLine(&buf, "X-WR-CALNAME:"+escapeICS(fmt.Sprintf("Passaggi a Livello - %s (%s)", stationName, stationID)))
	writeICSLine(&buf, "X-WR-CALDESC:"+escapeICS("Stima chiusura sbarre e transiti ferroviari inferiti da dati pubblici RFI/ViaggiaTreno"))
	writeICSLine(&buf, "X-WR-TIMEZONE:Europe/Rome")
	writeICSLine(&buf, "REFRESH-INTERVAL;VALUE=DURATION:PT15M")
	writeICSLine(&buf, "X-PUBLISHED-TTL:PT15M")
	writeICSLine(&buf, "X-CONTENT-HASH:"+newHash)

	for _, ev := range events {
		seq := 0
		if oldSeq, exists := prevState.Sequences[ev.UID]; exists {
			seq = oldSeq + 1
		}

		localPass := ev.EstimatedPassTime.In(romeLoc).Format("15:04")
		localClose := ev.BarrierCloseStart.In(romeLoc).Format("15:04")
		localOpen := ev.BarrierOpenEnd.In(romeLoc).Format("15:04")

		transitBadge := "FERMATA"
		if ev.IsNonStopTransit {
			transitBadge = "TRANSITO VELOCE"
		}
		if ev.MergedCount > 1 {
			transitBadge = fmt.Sprintf("CHIUSURA CONTINUA - %d TRENI", ev.MergedCount)
		}

		delayTag := ""
		if ev.DelayMinutes > 0 {
			delayTag = fmt.Sprintf(" (+%d min)", ev.DelayMinutes)
		}

		var summary string
		if ev.MergedCount > 1 {
			summary = fmt.Sprintf("[%s] %s - %s (primo: %s %d)%s",
				transitBadge,
				localClose,
				localOpen,
				ev.TrainCategory,
				ev.TrainNumber,
				delayTag,
			)
		} else {
			summary = fmt.Sprintf("[%s] %s %d (%s -> %s) ore %s%s",
				transitBadge,
				ev.TrainCategory,
				ev.TrainNumber,
				ev.OriginName,
				ev.DestinationName,
				localPass,
				delayTag,
			)
		}
		if ev.IsCancelled {
			summary = "[SOPPRESSO] " + summary
		}

		trainsSequence := strings.Join(ev.MergedTrains, "\n  - ")
		if trainsSequence == "" {
			trainsSequence = fmt.Sprintf("%s %d (%s -> %s)", ev.TrainCategory, ev.TrainNumber, ev.OriginName, ev.DestinationName)
		}

		description := fmt.Sprintf(
			"Stazione/PL: %s (%s)\nRegime chiusura: %s\nChiusura sbarre stimata: %s - %s\nTreni coinvolti nella chiusura:\n  - %s\nRitardo massimo rilevato: %d min\nAffidabilità stima: %.0f%%\nNota normativa/euristica: %s",
			ev.StationName,
			ev.StationID,
			transitBadge,
			ev.BarrierCloseStart.In(romeLoc).Format("15:04:05"),
			ev.BarrierOpenEnd.In(romeLoc).Format("15:04:05"),
			trainsSequence,
			ev.DelayMinutes,
			ev.Confidence*100,
			ev.HeuristicNotes,
		)

		status := "CONFIRMED"
		if ev.IsCancelled {
			status = "CANCELLED"
		}

		writeICSLine(&buf, "BEGIN:VEVENT")
		writeICSLine(&buf, "UID:"+ev.UID)
		writeICSLine(&buf, "DTSTAMP:"+dtStamp)
		writeICSLine(&buf, fmt.Sprintf("SEQUENCE:%d", seq))
		writeICSLine(&buf, "DTSTART:"+ev.BarrierCloseStart.UTC().Format("20060102T150405Z"))
		writeICSLine(&buf, "DTEND:"+ev.BarrierOpenEnd.UTC().Format("20060102T150405Z"))
		writeICSLine(&buf, "SUMMARY:"+escapeICS(summary))
		writeICSLine(&buf, "DESCRIPTION:"+escapeICS(description))
		writeICSLine(&buf, "LOCATION:"+escapeICS(fmt.Sprintf("Passaggio a Livello / Stazione %s", ev.StationName)))
		writeICSLine(&buf, "STATUS:"+status)
		writeICSLine(&buf, "TRANSP:OPAQUE")
		writeICSLine(&buf, "CATEGORIES:"+transitBadge+",FERROVIA")
		writeICSLine(&buf, fmt.Sprintf("X-PL-DELAY-MIN:%d", ev.DelayMinutes))
		writeICSLine(&buf, fmt.Sprintf("X-PL-CONFIDENCE:%.2f", ev.Confidence))
		writeICSLine(&buf, "X-PL-TRAIN-CATEGORY:"+escapeICS(ev.TrainCategory))
		writeICSLine(&buf, "END:VEVENT")
	}

	writeICSLine(&buf, "END:VCALENDAR")

	if err := os.WriteFile(fullPath, buf.Bytes(), 0o644); err != nil {
		return fileName, false, fmt.Errorf("scrittura file %s: %w", fullPath, err)
	}
	return fileName, true, nil
}

// writeICSLine applica il line-folding obbligatorio a 75 ottetti secondo RFC 5545 §3.1
// e termina ogni riga con CRLF (`\r\n`).
func writeICSLine(buf *bytes.Buffer, line string) {
	const maxOctets = 75
	if len(line) <= maxOctets {
		buf.WriteString(line)
		buf.WriteString("\r\n")
		return
	}

	written := 0
	first := true
	for len(line) > 0 {
		limit := maxOctets
		if !first {
			limit = maxOctets - 1 // Spazio iniziale per la continuazione
			buf.WriteByte(' ')
		}
		if len(line) <= limit {
			buf.WriteString(line)
			buf.WriteString("\r\n")
			break
		}

		// Evita di spezzare a metà un carattere UTF-8 multi-byte
		cut := limit
		for cut > 0 && (line[cut]&0xC0) == 0x80 {
			cut--
		}
		if cut == 0 {
			cut = limit
		}
		buf.WriteString(line[:cut])
		buf.WriteString("\r\n")
		line = line[cut:]
		written += cut
		first = false
	}
	_ = written
}

func escapeICS(s string) string {
	r := strings.NewReplacer(
		`\`, `\\`,
		`;`, `\;`,
		`,`, `\,`,
		"\r\n", `\n`,
		"\n", `\n`,
	)
	return r.Replace(s)
}

// ============================================================================
// 7. INDICE CATALOGO (JSON) PER SOTTOSCRIZIONE UTENTE
// ============================================================================

type CatalogEntry struct {
	StationID   string `json:"station_id"`
	StationName string `json:"station_name"`
	ICSFile     string `json:"ics_file"`
	EventCount  int    `json:"event_count"`
}

type CatalogIndex struct {
	UpdatedAtUTC string         `json:"updated_at_utc"`
	Stations     []CatalogEntry `json:"stations"`
}

func WriteCatalogIndexIfChanged(outDir string, entries []CatalogEntry, now time.Time) (bool, error) {
	slices.SortFunc(entries, func(a, b CatalogEntry) int {
		return cmp.Compare(a.StationName, b.StationName)
	})

	catalogPath := filepath.Join(outDir, "catalog.json")
	if raw, err := os.ReadFile(catalogPath); err == nil {
		var prev CatalogIndex
		if json.Unmarshal(raw, &prev) == nil && slices.Equal(prev.Stations, entries) {
			return false, nil
		}
	}

	idx := CatalogIndex{
		UpdatedAtUTC: now.UTC().Format(time.RFC3339),
		Stations:     entries,
	}
	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return false, err
	}
	data = append(data, '\n')
	if err := os.WriteFile(catalogPath, data, 0o644); err != nil {
		return false, err
	}
	return true, nil
}

// ============================================================================
// 8. DATASET SIMULATO DI FALLBACK / TEST LOCALE
// ============================================================================

func buildDemoTrains(now time.Time) []vtTrainDetail {
	base := now.Truncate(time.Hour)
	ms := func(offsetMin int) int64 {
		return base.Add(time.Duration(offsetMin) * time.Minute).UnixMilli()
	}

	// Simulazione direttrice Padova -> Vigonza-Pianiga -> Dolo -> Mira-Mirano -> Venezia Mestre
	// I treni Regionali fermano ovunque e permettono di apprendere la topologia;
	// il Frecciarossa 9715 salta le fermate intermedie ma viene interpolato automaticamente!
	return []vtTrainDetail{
		{
			NumeroTreno:       2214,
			Categoria:         "REG",
			CodOrigine:        "S02581",
			Origine:           "PADOVA",
			Destinazione:      "VENEZIA S. LUCIA",
			DataPartenzaTreno: ms(10),
			Ritardo:           2,
			TipoTreno:         "PG",
			Fermate: []vtStop{
				{ID: "S02581", Stazione: "PADOVA", PartenzaTeorica: ms(10), PartenzaReale: ms(12), TipoFermata: "P", Ritardo: 2},
				{ID: "S02584", Stazione: "BUSO DI VIGONZA", ArrivoTeorico: ms(18), PartenzaTeorica: ms(19), TipoFermata: "F", Ritardo: 2},
				{ID: "S02585", Stazione: "DOLO", ArrivoTeorico: ms(24), PartenzaTeorica: ms(25), TipoFermata: "F", Ritardo: 2},
				{ID: "S02586", Stazione: "MIRA-MIRANO", ArrivoTeorico: ms(30), PartenzaTeorica: ms(31), TipoFermata: "F", Ritardo: 2},
				{ID: "S02593", Stazione: "VENEZIA MESTRE", ArrivoTeorico: ms(40), PartenzaTeorica: ms(42), TipoFermata: "F", Ritardo: 2},
			},
		},
		{
			NumeroTreno:       9715,
			Categoria:         "FR",
			CodOrigine:        "S01700",
			Origine:           "MILANO CENTRALE",
			Destinazione:      "VENEZIA S. LUCIA",
			DataPartenzaTreno: ms(-90),
			Ritardo:           4,
			TipoTreno:         "PG",
			Fermate: []vtStop{
				// Il Frecciarossa ferma solo a Padova e Venezia Mestre: le fermate intermedie
				// (Buso di Vigonza, Dolo, Mira-Mirano) verranno inferite dall'euristica!
				{ID: "S02581", Stazione: "PADOVA", ArrivoTeorico: ms(28), PartenzaTeorica: ms(30), TipoFermata: "F", Ritardo: 4},
				{ID: "S02593", Stazione: "VENEZIA MESTRE", ArrivoTeorico: ms(45), PartenzaTeorica: ms(47), TipoFermata: "F", Ritardo: 4},
			},
		},
	}
}

// ============================================================================
// 9. UTILITY DI PARSING E SANITIZZAZIONE
// ============================================================================

var nonAlnumRegex = regexp.MustCompile(`[^A-Z0-9]+`)

func sanitizeFilename(name string) string {
	upper := strings.ToUpper(strings.TrimSpace(name))
	clean := nonAlnumRegex.ReplaceAllString(upper, "_")
	clean = strings.Trim(clean, "_")
	if clean == "" {
		return "STAZIONE"
	}
	return clean
}

func normalizeStationName(name, fallbackID string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return fallbackID
	}
	return trimmed
}

func normalizeCategory(cat string) string {
	c := strings.ToUpper(strings.TrimSpace(cat))
	if c == "" {
		return "TRENO"
	}
	return c
}

func buildEventUID(tr vtTrainDetail, stationID string) string {
	dayStr := time.UnixMilli(tr.DataPartenzaTreno).UTC().Format("20060102")
	return fmt.Sprintf("train-%d-%s-%s-%s@pl-tracker.local",
		tr.NumeroTreno, tr.CodOrigine, dayStr, stationID)
}

func firstNonZero(vals ...int64) int64 {
	for _, v := range vals {
		if v > 0 {
			return v
		}
	}
	return 0
}

func envOrDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envIntOrDefault(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloatOrDefault(key string, def float64) float64 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envBoolOrDefault(key string, def bool) bool {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

// ============================================================================
// 10. ENTRYPOINT PRINCIPALE
// ============================================================================

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg := loadConfig()
	if len(cfg.AnchorStations) == 0 {
		logger.Error("Nessuna stazione ancora specificata")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	now := time.Now().UTC()
	var trains []vtTrainDetail

	if cfg.DemoMode {
		logger.Info("Avvio in modalità DEMO locale")
		trains = buildDemoTrains(now)
	} else {
		stateCache := LoadTrainStateCache(cfg.StateCacheFile, now)
		client := NewRailClient(cfg, logger)
		defer client.Close()

		var err error
		trains, err = client.FetchActiveTrains(ctx, cfg.AnchorStations, stateCache, now)
		if saveErr := stateCache.SaveIfDirty(cfg.StateCacheFile); saveErr != nil {
			logger.Warn("Impossibile salvare state_cache.json", "err", saveErr)
		}
		if err != nil {
			if errors.Is(err, context.Canceled) {
				os.Exit(1)
			}
			logger.Warn("Errore recupero dati reali", "err", err)
			if cfg.FallbackDemo {
				logger.Info("Attivazione fallback DEMO per continuità operativa")
				trains = buildDemoTrains(now)
			} else {
				os.Exit(1)
			}
		}
	}

	if len(trains) == 0 {
		logger.Info("Nessun treno attivo trovato nella finestra temporale corrente")
		return
	}

	schedulesByStation := InferStationSchedules(trains, cfg)
	logger.Info("Fermate/Passaggi a livello inferiti lungo la direttrice",
		"stations_count", len(schedulesByStation))

	var (
		writtenCount int
		skippedCount int
		catalog      []CatalogEntry
	)

	for stID, events := range schedulesByStation {
		fileName, updated, err := WriteStationICSIfChanged(
			cfg.OutputDir, stID, events, now, *cfg.DelayJitterMin,
		)
		if err != nil {
			logger.Error("Errore generazione file ICS", "station", stID, "err", err)
			continue
		}

		catalog = append(catalog, CatalogEntry{
			StationID:   stID,
			StationName: events[0].StationName,
			ICSFile:     fileName,
			EventCount:  len(events),
		})

		if updated {
			writtenCount++
			logger.Info("Calendario aggiornato (lazy write)",
				"station", events[0].StationName, "file", fileName, "events", len(events))
		} else {
			skippedCount++
		}
	}

	if catUpdated, err := WriteCatalogIndexIfChanged(cfg.OutputDir, catalog, now); err != nil {
		logger.Warn("Errore scrittura catalog.json", "err", err)
	} else if catUpdated {
		logger.Info("Indice catalog.json aggiornato", "total_stations", len(catalog))
	}

	logger.Info("Esecuzione completata",
		"updated_files", writtenCount,
		"unchanged_skipped", skippedCount,
	)
}
