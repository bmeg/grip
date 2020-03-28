package web

import (
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"

	"crypto/tls"
)

type options struct {
	insecure bool
}

func getListOfStrings(url string, opts *options) ([]string, error) {

	client := resty.New()
	if opts != nil && opts.insecure {
		client.SetTLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	}
	q := client.R()

	resp, err := q.Get(url)
	if err != nil {
		return nil, err
	}

	txt := resp.Body()
	if txt[0] == '[' {
		data := []string{}
		err := json.Unmarshal(txt, &data)
		if err != nil {
			return nil, err
		}
		return data, nil
	}
	return nil, fmt.Errorf("Unable to parse results: %s", txt)
}
