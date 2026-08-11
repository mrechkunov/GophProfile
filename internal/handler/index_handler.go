package handler

import (
	"net/http"
)

func (h *AvatarHandler) IndexHandler(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "./web/static/index.html")
}
