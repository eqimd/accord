package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func SendGet(addr string, result any) error {
	r, err := http.Get(addr)
	if err != nil {
		return fmt.Errorf("http get error: %w", err)
	}
	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("cannot read body: %w", err)
	}

	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("not 200 code; body: %s", body)
	}

	if result != nil {
		if err := json.Unmarshal(body, result); err != nil {
			return fmt.Errorf("json unmarshal error: %w", err)
		}
	}

	return nil
}

func SendPost(addr string, reqBody any, result any) error {
	var reader io.Reader

	if reqBody != nil {
		bodyBytes, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("json marshal error: %w", err)
		}

		reader = bytes.NewReader(bodyBytes)
	}

	r, err := http.Post(addr, "application/json", reader)
	if err != nil {
		return fmt.Errorf("http get error: %w", err)
	}
	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("cannot read body: %w", err)
	}

	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("not 200 code; body: %s", body)
	}

	if result != nil {
		if err := json.Unmarshal(body, result); err != nil {
			return fmt.Errorf("json unmarshal error: %w", err)
		}
	}

	return nil
}
