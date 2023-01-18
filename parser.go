package mgodumper

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"

	"go.mongodb.org/mongo-driver/bson"
)

type Parser struct {
	file string
	gzip bool
}

func NewParser(file string, gzip bool) *Parser {
	return &Parser{file: file, gzip: gzip}
}

func (p *Parser) Start() (<-chan bson.Raw, error) {
	file, err := os.Open(p.file)
	if err != nil {
		return nil, err
	}

	var reader io.ReadCloser = file

	if p.gzip {
		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, err
		}

		reader = gzipReader
	}

	bsonReader := NewBSONSource(reader)
	ch := make(chan bson.Raw)

	go func() {
		for {
			data := bsonReader.LoadNext()
			if data == nil {
				break
			}

			var raw bson.Raw

			if err := bson.Unmarshal(data, &raw); err != nil {
				fmt.Println(err)
				continue
			}

			ch <- raw
		}

		close(ch)
	}()

	return ch, nil
}
