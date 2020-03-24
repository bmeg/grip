package tsv

import (
	"context"
	"log"

	"github.com/bmeg/grip/multi"
)

type TSVDriver struct {
	man        multi.Cache
	path       string
	lineIndex  multi.LineIndex
	idName     string
	idCol      int
	idxCols    []string
	idxMap     map[string]uint64
	header     []string
	lineReader *LineReader
	cparse     CSVParse
}

func TSVDriverBuilder(name string, url string, manager multi.Cache, opts multi.Options) (multi.Driver, error) {
	o := TSVDriver{path: url, idName: opts.PrimaryKey, idxCols: opts.IndexedColumns, man: manager}
	if err := o.Init(); err != nil {
		return nil, err
	}
	return &o, nil
}

var loaded = multi.AddDriver("tsv", TSVDriverBuilder)

func (t *TSVDriver) Close() error {
	return t.lineReader.Close()
}

func (t *TSVDriver) Init() error {

	hasHeader := false
	var err error
	t.lineReader, err = NewLineReader(t.path)
	if err != nil {
		return err
	}

	if i, err := t.man.GetLineIndex(t.path); err == nil {
		t.lineIndex = i
	} else {
		t.lineIndex, err = t.man.NewLineIndex(t.path)
		if err != nil {
			return err
		}
	}

	t.idxMap = map[string]uint64{}
	if _, err := t.lineIndex.GetLineCount(); err == nil {
		row := t.cparse.Parse(string(t.lineReader.SeekRead(0)))
		t.header = row
		for i := range row {
			if t.idName == row[i] {
				t.idCol = i
			}
			for j := range t.idxCols {
				if t.idxCols[j] == row[i] {
					t.idxMap[row[i]] = uint64(i)
				}
			}
		}
		//file have already been indexed
		return nil
	}

	for _, colName := range t.idxCols {
		t.lineIndex.AddIndexedField(colName)
	}

	count := uint64(0)
	t.lineIndex.IndexWrite(func(bl multi.LineIndexWriter) error {
		for line := range t.lineReader.ReadLines() {
			row := t.cparse.Parse(string(line.Text))
			if !hasHeader {
				t.header = row
				hasHeader = true
				for i := range row {
					if t.idName == row[i] {
						t.idCol = i
					}
					for j := range t.idxCols {
						if t.idxCols[j] == row[i] {
							t.idxMap[row[i]] = uint64(i)
						}
					}
				}
			} else {
				bl.SetIDLine(row[t.idCol], count)
				bl.SetLineOffset(count, line.Offset)
				iRow := map[string]interface{}{}
				for colName, col := range t.idxMap {
					iRow[colName] = row[col]
				}
				bl.IndexRow(count, iRow)
				count++
			}
		}
		bl.SetLineCount(count)
		return nil
	})
	log.Printf("SetupIndexCol: %d [%#v]", t.idCol, t.idxCols)
	log.Printf("Found %d rows", count)
	return nil
}

func (t *TSVDriver) GetRowByID(id string) (*multi.TableRow, error) {
	ln, err := t.lineIndex.GetIDLine(id)
	if err != nil {
		return nil, err
	}
	return t.GetLineRow(ln)
}

func (t *TSVDriver) GetLineText(lineNum uint64) ([]byte, error) {
	offset, err := t.lineIndex.GetLineOffset(lineNum)
	if err != nil {
		return nil, err
	}
	//cparse := CSVParse{}
	//log.Printf("LineOffset: %d", offset)
	return t.lineReader.SeekRead(offset), nil
}

func (t *TSVDriver) GetLineRow(lineNum uint64) (*multi.TableRow, error) {
	text, err := t.GetLineText(lineNum)
	if err != nil {
		return nil, err
	}
	r := t.cparse.Parse(string(text))
	d := map[string]interface{}{}
	for i := 0; i < len(t.header) && i < len(r); i++ {
		if i != t.idCol {
			d[t.header[i]] = r[i]
		}
	}
	o := multi.TableRow{r[t.idCol], d}
	return &o, nil
}

func (t *TSVDriver) GetRows(ctx context.Context) chan *multi.TableRow {
	log.Printf("ReadIndexCol: %d", t.idCol)
	out := make(chan *multi.TableRow, 10)
	go func() {
		defer close(out)
		hasHeader := false
		for line := range t.lineReader.ReadLines() {
			if !hasHeader {
				hasHeader = true
			} else {
				r := t.cparse.Parse(string(line.Text))
				d := map[string]interface{}{}
				for i := 0; i < len(t.header) && i < len(r); i++ {
					if i != t.idCol {
						d[t.header[i]] = r[i]
					}
				}
				//log.Printf("Key: %s", r[t.idCol])
				o := multi.TableRow{r[t.idCol], d}
				out <- &o
			}
		}
	}()
	return out
}

func (t *TSVDriver) GetIDs(ctx context.Context) chan string {
	return t.lineIndex.GetIDChannel(ctx)
}

func (t *TSVDriver) GetRowsByField(ctx context.Context, field string, value string) chan *multi.TableRow {
	out := make(chan *multi.TableRow, 10)
	go func() {
		defer close(out)
		for line := range t.lineIndex.GetLinesByField(ctx, field, value) {
			o, err := t.GetLineRow(line)
			if err == nil {
				out <- o
			}
		}
	}()
	return out
}
