package util

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"google.golang.org/protobuf/encoding/protojson"
)

// StreamLines returns a channel of lines from a file.
func StreamLines(file string, chanSize int) (chan string, error) {

	var fh io.ReadCloser
	var err error
	if strings.HasPrefix(file, "s3+http://") || strings.HasPrefix(file, "s3+https://") {
		u, err := url.Parse(file)
		if err != nil {
			return nil, err
		}
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

		minioClient, err := minio.New(u.Host, &minio.Options{
			Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
			Secure: useSSL,
		})
		if err != nil {
			return nil, err
		}
		spath := strings.SplitN(u.Path, "/", 3)
		if len(spath) != 3 {
			return nil, fmt.Errorf("incorrectly formatted path: %s", u.Path)
		}
		log.Info("Downloading S3: %s %s", spath[1], spath[2])
		obj, err := minioClient.GetObject(context.Background(), spath[1], spath[2], minio.GetObjectOptions{})
		if err != nil {
			return nil, err
		}
		fh = obj
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

// StreamVerticesFromFile reads a file containing a vertex per line and
// streams *gripql.Vertex objects out on a channel
func StreamVerticesFromFile(file string, workers int) (chan *gripql.Vertex, error) {
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

	vertChan := make(chan *gripql.Vertex, workers)
	var wg sync.WaitGroup

	jum := protojson.UnmarshalOptions{DiscardUnknown: true}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			for line := range lineChan {
				v := &gripql.Vertex{}
				err := jum.Unmarshal([]byte(line), v)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Errorf("Unmarshaling vertex: %s", line)
				} else {
					vertChan <- v
				}
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(vertChan)
	}()

	return vertChan, nil
}

// StreamEdgesFromFile reads a file containing an edge per line and
// streams gripql.Edge objects on a channel
func StreamEdgesFromFile(file string, workers int) (chan *gripql.Edge, error) {
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

	edgeChan := make(chan *gripql.Edge, workers)
	var wg sync.WaitGroup

	jum := protojson.UnmarshalOptions{DiscardUnknown: true}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			for line := range lineChan {
				e := &gripql.Edge{}
				err := jum.Unmarshal([]byte(line), e)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Errorf("Unmarshaling edge: %s", line)
				} else {
					edgeChan <- e
				}
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(edgeChan)
	}()

	return edgeChan, nil
}
