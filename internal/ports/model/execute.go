package model

type ExecuteRequest struct {
	Query string `json:"query"`
}

type ExecuteResponse struct {
	Response string `json:"response"`
}
