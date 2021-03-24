package jobstorage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"

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
	Stream(graph, id string) (*Stream, error)
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
			sData, err := ioutil.ReadAll(file)
			if err == nil {
				job := Job{}
				err := json.Unmarshal(sData, &job)
				if err == nil {
					out.jobs.Store(jobKey(graphName, jobName), &job)
				}
			}
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
	spoolDir, err := ioutil.TempDir(graphDir, "job-")
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
		Status:        gripql.JobStatus{Query: stream.Query, Id: jobName, Graph: graph},
		DataType:      stream.DataType,
		MarkTypes:     stream.MarkTypes,
		StepChecksums: cs,
	}
	fs.jobs.Store(jobKey(graph, jobName), job)
	go func() {
		job.Status.State = gripql.JobState_RUNNING
		log.Printf("Starting Job: %#v", job)
		defer resultFile.Close()
		for i := range stream.Pipe {
			out, err := json.Marshal(i)
			if err == nil {
				resultFile.Write([]byte(fmt.Sprintf("%s\n", out)))
			} else {
				log.Printf("Marshal Error: %s", err)
			}
			job.Status.Count += 1
		}
		statusPath := filepath.Join(spoolDir, "status")
		statusFile, err := os.Create(statusPath)
		if err == nil {
			defer statusFile.Close()
			out, err := json.Marshal(job)
			if err == nil {
				statusFile.Write([]byte(fmt.Sprintf("%s\n", out)))
			}
			job.Status.State = gripql.JobState_COMPLETE
			log.Printf("Job Done: %s (%d results)", jobName, job.Status.Count)
		} else {
			job.Status.State = gripql.JobState_ERROR
			log.Printf("Job Error: %s %s", jobName, err)
		}
	}()
	return jobName, nil
}

func (fs *FSResults) Stream(graph, id string) (*Stream, error) {
	if v, ok := fs.jobs.Load(jobKey(graph, id)); ok {
		vJob := v.(*Job)
		if vJob.Status.State == gripql.JobState_COMPLETE {
			out := make(chan *gdbi.Traveler, 10)
			resultFile := filepath.Join(fs.BaseDir, sanitize.Name(graph), sanitize.Name(id), "results")
			results, err := os.Open(resultFile)
			if err != nil {
				return nil, err
			}
			go func() {
				defer close(out)
				defer results.Close()
				scan := bufio.NewScanner(results)
				bufSize := 1024 * 1024 * 32
				buf := make([]byte, bufSize)
				scan.Buffer(buf, bufSize)
				for scan.Scan() {
					t := gdbi.Traveler{}
					err := json.Unmarshal([]byte(scan.Text()), &t)
					if err == nil {
						out <- &t
					}
				}
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
