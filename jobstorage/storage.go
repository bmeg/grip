package jobstorage

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"

	"github.com/kennygrant/sanitize"
)

type Stream struct {
	Pipe      gdbi.InPipe
	DataType  gdbi.DataType
	MarkTypes map[string]gdbi.DataType
	Query     []*gripql.GraphStatement
}

type JobStorage interface {
	List(graph string) (chan string, error)
	Search(graph string, Query []*gripql.GraphStatement) (chan *gripql.JobStatus, error)
	Spool(graph string, stream *Stream) (string, error)
	Stream(ctx context.Context, graph, id string) (*Stream, error)
	Delete(graph, id string) error
	Status(graph, id string) (*gripql.JobStatus, error)
}

type Job struct {
	Status        gripql.JobStatus
	DataType      gdbi.DataType
	MarkTypes     map[string]gdbi.DataType
	StepChecksums []string
}

func jobKey(graph, job string) string {
	return fmt.Sprintf("%s/%s", sanitize.Name(graph), sanitize.Name(job))
}

func NewFSJobStorage(path string) *FSResults {
	out := FSResults{path, &sync.Map{}}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.MkdirAll(path, 0700)
	}
	statusGlob := filepath.Join(path, "*", "*", "status")
	matches, _ := filepath.Glob(statusGlob)
	for _, j := range matches {
		jobDir := filepath.Dir(j)
		graphDir := filepath.Dir(jobDir)
		jobName := filepath.Base(jobDir)
		graphName := filepath.Base(graphDir)
		file, err := os.Open(j)
		if err == nil {
			sData, err := io.ReadAll(file)
			if err == nil {
				job := Job{}
				err := json.Unmarshal(sData, &job)
				if err == nil {
					log.Infof("Found job %s %s", graphName, jobName)
					out.jobs.Store(jobKey(graphName, jobName), &job)
				} else {
					log.Infof("Error Unmarshaling job data: %s", err)
				}
			} else {
				log.Infof("Error reading job data: %s", err)
			}
		} else {
			log.Infof("Error opening job data: %s", err)
		}
	}
	return &out
}

type FSResults struct {
	BaseDir string
	jobs    *sync.Map
}

func (fs *FSResults) List(graph string) (chan string, error) {
	out := make(chan string)
	go func() {
		defer close(out)
		fs.jobs.Range(func(key, value interface{}) bool {
			vJob := value.(*Job)
			if vJob.Status.Graph == graph {
				out <- vJob.Status.Id
			}
			return true
		})
	}()
	return out, nil
}

func (fs *FSResults) Search(graph string, Query []*gripql.GraphStatement) (chan *gripql.JobStatus, error) {
	out := make(chan *gripql.JobStatus)
	qcs, _ := TraversalChecksum(Query)
	go func() {
		defer close(out)
		fs.jobs.Range(func(key, value interface{}) bool {
			vJob := value.(*Job)
			if vJob.Status.Graph == graph {
				if JobMatch(qcs, vJob.StepChecksums) {
					out <- &vJob.Status
				}
			}
			return true
		})
	}()
	return out, nil
}

func (fs *FSResults) Spool(graph string, stream *Stream) (string, error) {
	graphDir := filepath.Join(fs.BaseDir, sanitize.Name(graph))
	if _, err := os.Stat(graphDir); os.IsNotExist(err) {
		os.MkdirAll(graphDir, 0700)
	}
	spoolDir, err := os.MkdirTemp(graphDir, "job-")
	if err != nil {
		return "", err
	}
	jobName := filepath.Base(spoolDir)
	if _, err := os.Stat(spoolDir); os.IsNotExist(err) {
		os.MkdirAll(spoolDir, 0700)
	}
	resultPath := filepath.Join(spoolDir, "results")
	resultFile, err := os.Create(resultPath)
	if err != nil {
		return "", err
	}

	cs, _ := TraversalChecksum(stream.Query)
	job := &Job{
		Status:        gripql.JobStatus{Query: stream.Query, Id: jobName, Graph: graph, Timestamp: time.Now().Format(time.RFC3339)},
		DataType:      stream.DataType,
		MarkTypes:     stream.MarkTypes,
		StepChecksums: cs,
	}
	fs.jobs.Store(jobKey(graph, jobName), job)
	tbStream := MarshalStream(stream.Pipe, 4) //TODO: make worker count configurable
	go func() {
		job.Status.State = gripql.JobState_RUNNING
		log.Infof("Starting Job: %#v", job)
		defer resultFile.Close()
		for i := range tbStream {
			resultFile.Write(i)
			resultFile.Write([]byte("\n"))
			job.Status.Count += 1
		}
		statusPath := filepath.Join(spoolDir, "status")
		statusFile, err := os.Create(statusPath)
		if err == nil {
			defer statusFile.Close()
			job.Status.State = gripql.JobState_COMPLETE
			out, err := json.Marshal(job)
			if err == nil {
				statusFile.Write([]byte(fmt.Sprintf("%s\n", out)))
			}
			log.Infof("Job Done: %s (%d results)", jobName, job.Status.Count)
		} else {
			job.Status.State = gripql.JobState_ERROR
			log.Infof("Job Error: %s %s", jobName, err)
		}
	}()
	return jobName, nil
}

func (fs *FSResults) Stream(ctx context.Context, graph, id string) (*Stream, error) {
	if v, ok := fs.jobs.Load(jobKey(graph, id)); ok {
		vJob := v.(*Job)
		if vJob.Status.State == gripql.JobState_COMPLETE {
			resultFile := filepath.Join(fs.BaseDir, sanitize.Name(graph), sanitize.Name(id), "results")
			results, err := os.Open(resultFile)
			if err != nil {
				return nil, err
			}
			os := make(chan []byte, 40)
			out := UnmarshalStream(os, 4) //TODO: make worker count configurable
			go func() {
				defer close(os)
				defer results.Close()
				scan := bufio.NewScanner(results)
				bufSize := 1024 * 1024 * 32
				buf := make([]byte, bufSize)
				scan.Buffer(buf, bufSize)
				count := uint64(0)
				for scan.Scan() {
					if ctx.Err() == context.Canceled {
						return
					}
					c := make([]byte, len(scan.Bytes()))
					copy(c, scan.Bytes())
					count++
					os <- c
				}
				log.Infof("Stored job with %d records read", count)
			}()
			return &Stream{
				Pipe:      out,
				DataType:  vJob.DataType,
				MarkTypes: vJob.MarkTypes,
			}, nil
		}
		return nil, fmt.Errorf("Job %s not complete", id)
	}
	return nil, fmt.Errorf("Job Not Found")
}

func (fs *FSResults) Delete(graph, id string) error {
	if v, ok := fs.jobs.Load(jobKey(graph, id)); ok {
		vJob := v.(*Job)
		if vJob.Status.State == gripql.JobState_RUNNING || vJob.Status.State == gripql.JobState_QUEUED {
			return fmt.Errorf("Job cancel not yet implemented")
		}
		fs.jobs.Delete(jobKey(graph, id))
		jobDir := filepath.Join(fs.BaseDir, sanitize.Name(graph), sanitize.Name(id))
		os.RemoveAll(jobDir)
	}
	return nil
}

func (fs *FSResults) Status(graph, id string) (*gripql.JobStatus, error) {
	if v, ok := fs.jobs.Load(jobKey(graph, id)); ok {
		vJob := v.(*Job)
		a := vJob.Status
		return &a, nil
	}
	return nil, fmt.Errorf("Job Not Found")
}
