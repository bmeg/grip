package web

import (
  "log"
  "fmt"
  "strings"
  "context"
  "github.com/bmeg/grip/tabular"
  "github.com/mitchellh/mapstructure"
  "github.com/oliveagle/jsonpath"
  "github.com/aymerick/raymond"

  "crypto/tls"
  "github.com/go-resty/resty/v2"

)


type Driver struct {
  name  string
  conf Config
  opts tabular.Options
  cache      tabular.Cache
  rowStorage tabular.RowStorage
}

type QueryConfig struct {
  URL         string  `json:"url"`
  ElementList string  `json:"elementList"`
  Element     string  `json:"element"`
  Params      map[string]string `json:"params"`
  Headers     []string          `json:"headers"`
  Cache       bool              `json:"cache"`
  Insecure    bool              `json:"insecure"`
}

type Config struct {
  List  *QueryConfig             `json:"list"`
  Get   map[string]*QueryConfig  `json:"get"`
}

func WebDriverBuilder(name string, url string, manager tabular.Cache, opts tabular.Options) (tabular.Driver, error) {
  o := Driver{name:name, opts:opts, cache:manager}
  conf := Config{}
  err := mapstructure.Decode(opts.Config, &conf)
  if err != nil {
    log.Printf("Error: %s", err)
  }
  log.Printf("Web Config: %s", conf)
  o.conf = conf
  return &o, nil
}

var loaded = tabular.AddDriver("web", WebDriverBuilder)

func (d *Driver) GetIDs(ctx context.Context) chan string {
  return nil
}

func pathFix(p string) string {
  if !strings.HasPrefix(p, "$.") {
    return "$." + p
  }
  return p
}

func (d *Driver) buildCache() {
  if d.conf.List != nil && d.conf.List.Cache {
    url := d.name + ":" + d.conf.List.URL
    if d.rowStorage == nil {
      if r, err := d.cache.GetRowStorage(url); err != nil {
        if r, err := d.cache.NewRowStorage(url); err != nil {
          log.Printf("Error creating row storage")
          return
        } else {
          d.rowStorage = r
        }
      } else {
        d.rowStorage = r
      }
      log.Printf("Caching %s", d.conf.List.URL)
      for row := range d.fetchRows(context.TODO()) {
        d.rowStorage.Write(row)
      }
    }
  }
}

func (d *Driver) fetchRows(ctx context.Context) chan *tabular.TableRow {
  out := make(chan *tabular.TableRow, 10)
  go func() {
    defer close(out)

    log.Printf("Getting Rows from %s", d.conf.List.URL)

    data := map[string]interface{}{}

    client := resty.New()
    if d.conf.List.Insecure {
      client.SetTLSClientConfig( &tls.Config{InsecureSkipVerify: true} )
    }
    q := client.R()
    if len(d.conf.List.Params) > 0 {
      q = q.SetQueryParams(d.conf.List.Params)
    }
    resp, err := q.SetResult(&data).
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
      select {
      case <-ctx.Done():
      default:
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
    }
  }()
  return out
}

func (d *Driver) GetRows(ctx context.Context) chan *tabular.TableRow {
  d.buildCache()
  if d.rowStorage != nil {
    return d.rowStorage.GetRowsByField(ctx, "", "")
  }
  return d.fetchRows(ctx)
}

func (d *Driver) GetRowByID(id string) (*tabular.TableRow, error) {
  d.buildCache()
  if d.rowStorage != nil {
    return d.rowStorage.GetRowByID(id)
  }
  log.Printf("Getting row: %s", id)
  if tableGet, ok := d.conf.Get[d.opts.PrimaryKey]; ok {
    params := map[string]string{}
    for k, v := range tableGet.Params {
      ctx := map[string]string{
          "id": id,
      }
      result, err := raymond.Render(v, ctx)
      if err == nil {
        params[k] = result
      } else {
        log.Printf("Template error: %s", err)
      }
    }
    data := map[string]interface{}{}

    q := resty.New().R()
    if len(params) > 0 {
      q = q.SetQueryParams(params)
    }
    for _, h := range tableGet.Headers {
      t := strings.Split(h, ":")
      q = q.SetHeader(t[0], t[1])
    }

    ctx := map[string]string{
        "id": id,
    }
    url, err := raymond.Render(tableGet.URL, ctx)
    if err != nil {
      log.Printf("Template error: %s", err)
    }
    resp, err := q.SetResult(&data).
    		Get(url)
    if err != nil {
      return nil, err
    }
    resp.Result()
    row, err := jsonpath.JsonPathLookup(data, tableGet.Element )
    if rowData, ok := row.(map[string]interface{}); ok {
      gid, err := jsonpath.JsonPathLookup(rowData, pathFix(d.opts.PrimaryKey) )
      if err != nil {
        log.Printf("Data error: %s", err)
        return nil, err
      }
      if gidStr, ok := gid.(string); ok {
        return &tabular.TableRow{ gidStr, rowData }, nil
      }
    }
  }
  return nil, fmt.Errorf("Getter for %s not found", d.opts.PrimaryKey)
}

func (d *Driver) GetRowsByField(ctx context.Context, field string, value string) chan *tabular.TableRow {

  d.buildCache()
  if d.rowStorage != nil {
    return d.rowStorage.GetRowsByField(ctx, field, value)
  }

  log.Printf("Getting rows by field: %s = %s (primaryKey: %s)", field, value, d.opts.PrimaryKey)
  out := make(chan *tabular.TableRow, 10)
  go func() {
    defer close(out)

    if tableGet, ok := d.conf.Get[field]; ok {
      params := map[string]string{}
      for k, v := range tableGet.Params {
        ctx := map[string]string{
            "id": value,
        }
        result, err := raymond.Render(v, ctx)
        if err == nil {
          params[k] = result
        } else {
          log.Printf("Template error: %s", err)
        }
      }
      data := map[string]interface{}{}

      q := resty.New().R()
      if len(params) > 0 {
        q = q.SetQueryParams(params)
      }
      resp, err := q.SetResult(&data).
      		Get(tableGet.URL)
      if err != nil {
        log.Printf("Error: %s", err)
        return
      }
      resp.Result()
      //log.Printf("Got: %s", data)
      res, err := jsonpath.JsonPathLookup(data, tableGet.ElementList )
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
          if err == nil {
            if gidStr, ok := gid.(string); ok {
              out <- &tabular.TableRow{ gidStr, rowData }
            }
          }
        }
      }
    } else {
      log.Printf("Getter for %s not found", field)
    }
  }()
  return out
}
