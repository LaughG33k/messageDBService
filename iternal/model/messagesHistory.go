package model

type MessageHistory struct {
	Received map[string]map[string]map[string]string `json:"received"`
	Sent     map[string]map[string]map[string]string `json:"sent"`
}
