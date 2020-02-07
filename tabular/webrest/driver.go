package webrest

import (
  "log"
  "strings"
  "context"
  "github.com/bmeg/grip/tabular"
  "github.com/mitchellh/mapstructure"
  "github.com/oliveagle/jsonpath"

  "github.com/go-resty/resty/v2"
)


type Driver struct {
  conf Config
  opts tabular.Options
}

type QueryConfig struct {
  URL         string  `json:"url"`
  ElementList string  `json:"elementList"`
}

type Config struct {
  List  *QueryConfig   `json:"list"`
}

func TSVDriverBuilder(url string, manager *tabular.TableManager, opts tabular.Options) (tabular.Driver, error) {
  o := Driver{opts:opts}
  conf := Config{}
  err := mapstructure.Decode(opts.Config, &conf)
  if err != nil {
    log.Printf("Error: %s", err)
  }
  log.Printf("Web Config: %s", conf)
  o.conf = conf
  return &o, nil
}

var loaded = tabular.AddDriver("webrest", TSVDriverBuilder)



func (d *Driver) GetIDs(ctx context.Context) chan string {
  return nil
}

func pathFix(p string) string {
  if !strings.HasPrefix(p, "$.") {
    return "$." + p
  }
  return p
}

func (d *Driver) GetRows(ctx context.Context) chan *tabular.TableRow {
  out := make(chan *tabular.TableRow, 10)
  go func() {
    defer close(out)

    log.Printf("Getting Rows from %s", d.conf.List.URL)
    client := resty.New()

    data := map[string]interface{}{}

    resp, err := client.R().
        SetResult(&data).
    		Get(d.conf.List.URL)

    if err != nil {
      log.Printf("Error: %s", err)
      return
    }
    resp.Result()
    res, err := jsonpath.JsonPathLookup(data, d.conf.List.ElementList )
    if err != nil {
      log.Printf("Error: %s", err)
      return
    }

    resList, ok := res.([]interface{})
    if !ok {
      return
    }

    for _, row := range resList {
      if rowData, ok := row.(map[string]interface{}); ok {
        gid, err := jsonpath.JsonPathLookup(rowData, pathFix(d.opts.PrimaryKey) )
        if err != nil {
          log.Printf("Error: %s", err)
        }
        if gidStr, ok := gid.(string); ok {
          o := tabular.TableRow{ gidStr, rowData }
          out <- &o
        }
      }
    }
  }()

  return out
}

func (d *Driver) GetRowByID(id string) (*tabular.TableRow, error) {
  return nil, nil
}

func (d *Driver) GetRowsByField(ctx context.Context, field string, value string) chan *tabular.TableRow {
  return nil
}
