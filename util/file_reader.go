package util

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"google.golang.org/protobuf/types/known/structpb"
)

func getS3Client(u *url.URL) (*minio.Client, error) {

	useSSL := false
	if u.Scheme == "s3+https" {
		useSSL = true
	}

	accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	if accessKeyID == "" {
		log.Info("AWS_ACCESS_KEY_ID not set")
	}
	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if secretAccessKey == "" {
		log.Info("AWS_SECRET_ACCESS_KEY not set")
	}

	mc, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	return mc, err
}

func getS3URL(path string) *url.URL {
	if strings.HasPrefix(path, "s3+http://") || strings.HasPrefix(path, "s3+https://") {
		u, err := url.Parse(path)
		if err != nil {
			return nil
		}
		return u
	}
	return nil
}

func StreamDataFromFile(file string, workers int) (chan string, error) {
	fi, err := os.Stat(file)
	if err != nil {
		return nil, err
	}
	if fi.Size() == 0 {
		return nil, fmt.Errorf("file is empty: %s", file)
	}

	lineChan := make(chan string, workers*2) // Use a buffered channel for better performance
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(lineChan)

		fh, err := os.Open(file)
		if err != nil {
			fmt.Printf("ERROR opening file: %v\n", err)
			return
		}
		defer fh.Close()

		reader := bufio.NewReader(fh)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					fmt.Printf("ERROR reading file: %v\n", err)
				}
				break
			}
			line = strings.TrimSpace(line)
			if len(line) > 0 {
				lineChan <- line
			}
		}
	}()

	return lineChan, nil
}

// StreamLines returns a channel of lines from a file.
func StreamLines(file string, chanSize int) (chan string, error) {
	var fh io.ReadCloser
	var err error
	if u := getS3URL(file); u != nil {
		if minioClient, err := getS3Client(u); err != nil {
			return nil, err
		} else {
			spath := strings.SplitN(u.Path, "/", 3)
			if len(spath) != 3 {
				return nil, fmt.Errorf("incorrectly formatted path: %s", u.Path)
			}
			log.Infof("Downloading S3: %s %s", spath[1], spath[2])
			obj, err := minioClient.GetObject(context.Background(), spath[1], spath[2], minio.GetObjectOptions{})
			if err != nil {
				return nil, err
			}
			fh = obj
		}
	} else {
		fh, err = os.Open(file)
		if err != nil {
			return nil, err
		}
	}
	var scanner *bufio.Scanner

	if strings.HasSuffix(file, ".gz") {
		gz, err := gzip.NewReader(fh)
		if err != nil {
			return nil, err
		}
		scanner = bufio.NewScanner(gz)
	} else {
		scanner = bufio.NewScanner(fh)
	}

	const maxCapacity = 16 * 1024 * 1024
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, maxCapacity)

	lineChan := make(chan string, chanSize)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			lineChan <- line
		}
		if err := scanner.Err(); err != nil {
			log.WithFields(log.Fields{"error": err}).Errorf("Reading file: %s", file)
		}
		close(lineChan)
		fh.Close()
	}()

	return lineChan, nil
}

func StreamRawJsonFromFile(file string, workers int, graph string, extra_args map[string]any) (chan *gripql.RawJson, error) {
	if workers < 1 {
		workers = 1
	}
	if workers > 99 {
		workers = 99
	}
	lineChan, err := StreamLines(file, workers)
	if err != nil {
		return nil, err
	}
	jsonChan := make(chan *gripql.RawJson, workers)
	var wg sync.WaitGroup
	jum := gripql.NewFlattenMarshaler()

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range lineChan {
				rawData := &gripql.RawJson{
					Data:      &structpb.Struct{},
					ExtraArgs: &structpb.Struct{},
					Graph:     graph,
				}

				rawData.ExtraArgs, err = structpb.NewStruct(extra_args)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Errorf("Marshalling ExtraArgs: %#v", extra_args)
					continue
				}
				err = jum.Unmarshal([]byte(line), rawData.Data)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Errorf("Unmarshaling vertex: %s", line)
					continue
				}
				jsonChan <- rawData
			}
		}()
	}
	go func() {
		wg.Wait()
		close(jsonChan)
	}()
	return jsonChan, nil
}

// StreamEdgesFromFile reads a file of JSON edges and streams *gripql.Edge objects.
func StreamEdgesFromFile(file string, workers int) (chan *gripql.Edge, error) {
	lineChan, err := StreamDataFromFile(file, workers)
	if err != nil {
		return nil, err
	}
	edgeChan := make(chan *gripql.Edge, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			jum := gripql.NewFlattenMarshaler()
			for line := range lineChan {
				e := &gripql.Edge{}
				err := jum.Unmarshal([]byte(line), e)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Errorf("Unmarshaling edge: %s", line)
				} else {
					edgeChan <- e
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(edgeChan)
	}()
	return edgeChan, nil
}

// StreamVerticesFromFile reads a file of JSON vertices and streams *gripql.Vertex objects.
func StreamVerticesFromFile(file string, workers int) (chan *gripql.Vertex, error) {
	lineChan, err := StreamDataFromFile(file, workers)
	if err != nil {
		return nil, err
	}
	vertChan := make(chan *gripql.Vertex, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			jum := gripql.NewFlattenMarshaler()
			for line := range lineChan {
				v := &gripql.Vertex{}
				err := jum.Unmarshal([]byte(line), v)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Errorf("Unmarshaling edge: %s", line)
				} else {
					vertChan <- v
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(vertChan)
	}()

	return vertChan, nil
}

func DirScan(baseDir string, fileGlob string) ([]string, error) {
	if u := getS3URL(baseDir); u != nil {
		if client, err := getS3Client(u); err == nil {
			log.Infof("Scanning %s", u)
			out := []string{}
			spath := strings.SplitN(u.Path, "/", 3)
			if len(spath) != 3 {
				return nil, fmt.Errorf("incorrectly formatted path: %s", u.Path)
			}
			dirPrefix := spath[2]
			if !strings.HasSuffix(dirPrefix, "/") {
				dirPrefix = dirPrefix + "/"
			}
			globStr := dirPrefix + fileGlob
			log.Infof("GlobStr: %s", globStr)
			log.Infof("Listing %s %s", spath[1], dirPrefix)
			for obj := range client.ListObjects(context.Background(), spath[1],
				minio.ListObjectsOptions{
					Recursive: true,
					Prefix:    dirPrefix,
				}) {
				if match, err := filepath.Match(globStr, obj.Key); match && err == nil {
					out = append(out, fmt.Sprintf("%s://%s/%s/%s", u.Scheme, u.Host, spath[1], obj.Key))
				} else if err != nil {
					log.Infof("Match error: %s", err)
				}
			}
			return out, nil
		} else {
			return nil, err
		}
	}
	return filepath.Glob(filepath.Join(baseDir, fileGlob))
}
