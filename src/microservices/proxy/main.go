package main

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
)

type HttpProxy struct {
	gradualMigration       bool
	moviesMigrationPercent int

	moviesServiceUrl string // http://movies-service:8081
	monolithUrl      string // http://monolith:8080
}

func (h HttpProxy) proxy(w http.ResponseWriter, r *http.Request) {
	randomN := rand.Int64N(101)
	if h.gradualMigration && r.URL.Path == "/api/movies" {
		if int(randomN) < h.moviesMigrationPercent {
			http.Redirect(w, r, fmt.Sprintf("%s/%s", h.moviesServiceUrl, r.URL.Path), http.StatusFound)
			return
		}

		http.Redirect(w, r, fmt.Sprintf("%s/%s", h.monolithUrl, r.URL.Path), http.StatusFound)
		return
	}

	http.Redirect(w, r, fmt.Sprintf("%s/%s", h.monolithUrl, r.URL.Path), http.StatusFound)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func main() {
	port := os.Getenv("PORT")
	monolithUrl := os.Getenv("MONOLITH_URL")
	moviesServiceUrl := os.Getenv("MOVIES_SERVICE_URL")

	gradualMigration, err := strconv.ParseBool(os.Getenv("GRADUAL_MIGRATION"))
	if err != nil {
		panic(err)
	}

	moviesMigrationPercent, err := strconv.Atoi(os.Getenv("MOVIES_MIGRATION_PERCENT"))
	if err != nil {
		panic(err)
	}

	httpProxy := HttpProxy{
		gradualMigration:       gradualMigration,
		moviesMigrationPercent: moviesMigrationPercent,
		moviesServiceUrl:       moviesServiceUrl,
		monolithUrl:            monolithUrl,
	}

	http.HandleFunc("/", httpProxy.proxy)
	http.HandleFunc("/health", handleHealth)

	http.ListenAndServe(":"+port, nil)
}
