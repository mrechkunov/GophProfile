package main

import (
	"fmt"
	"gophprofile/internal/config"
	"log"
	"net/http"
)

func main() {
	config.Init()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Avatar Service is running4!")
	})
	log.Println("Starting server on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
