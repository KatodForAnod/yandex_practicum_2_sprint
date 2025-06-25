package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	host := h.monolithUrl
	if h.gradualMigration && r.URL.Path == "/api/movies" {
		if int(randomN) < h.moviesMigrationPercent {
			host = h.moviesServiceUrl
		} else {
			host = h.monolithUrl
		}
	}

	newReq, err := http.NewRequest(r.Method, fmt.Sprintf("%s/%s", host, r.URL.Path), r.Body)
	if err != nil {
		panic(err)
	}
	for header, values := range r.Header {
		for _, value := range values {
			newReq.Header.Add(header, value)
		}
	}
	//newReq.Host = host
	newReq.RemoteAddr = r.RemoteAddr

	client := http.Client{}
	resp, err := client.Do(newReq)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
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
