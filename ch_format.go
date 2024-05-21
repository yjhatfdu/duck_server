package main

import (
	"database/sql/driver"
	"encoding/csv"
	"errors"
	"github.com/goccy/go-json"
	"io"
)

type ClickhouseFormatWriter interface {
	Write(value []any) error
	io.Closer
}

type ClickhouseFormatReader interface {
	Read([]driver.Value) error
	io.Closer
}

type ClickhouseFormatReaderFactory func(columnNames, columnTypes []string, reader io.Reader) (ClickhouseFormatReader, error)

type ClickhouseFormatWriterFactory func(columnNames, columnTypes []string, writer io.Writer) (ClickhouseFormatWriter, error)

func newJsonLinesFormatReader(columnNames, columnTypes []string, reader io.Reader) (ClickhouseFormatReader, error) {
	decoder := json.NewDecoder(reader)
	return &JsonLinesFormatReader{
		columns:  columnNames,
		decoder:  decoder,
		receiver: make(map[string]any, len(columnNames)),
	}, nil
}

type JsonLinesFormatReader struct {
	columns  []string
	decoder  *json.Decoder
	receiver map[string]any
	closer   io.Closer
}

func (j *JsonLinesFormatReader) Read(value []driver.Value) error {
	err := j.decoder.Decode(&j.receiver)
	if err != nil {
		return err

	}
	if len(j.columns) != len(value) {
		return errors.New("column length mismatch")
	}
	for i, column := range j.columns {
		value[i] = j.receiver[column]
	}
	return nil
}

func (j *JsonLinesFormatReader) Close() error {
	return j.closer.Close()
}

func newJsonLinesFormatWriter(columnNames, columnTypes []string, writer io.Writer) (ClickhouseFormatWriter, error) {
	encoder := json.NewEncoder(writer)
	return &JsonLinesFormatWriter{
		columns: columnNames,
		encoder: encoder,
		m:       make(map[string]any, len(columnNames)),
	}, nil
}

type JsonLinesFormatWriter struct {
	columns []string
	encoder *json.Encoder
	m       map[string]any
}

func (j *JsonLinesFormatWriter) Write(value []any) error {
	for i, column := range j.columns {
		j.m[column] = value[i]
	}
	return j.encoder.Encode(j.m)
}

func (j *JsonLinesFormatWriter) Close() error {
	return nil
}

func newCSVFormatReaderGeneric(columnNames, columnTypes []string, reader io.Reader, sep rune, header bool) (ClickhouseFormatReader, error) {
	r := csv.NewReader(reader)
	r.ReuseRecord = true
	r.Comma = sep
	if header {
		_, err := r.Read()
		if err != nil {
			return nil, err
		}
	}
	columnParsers := make([]func(string) (driver.Value, error), len(columnTypes))
	for i, columnType := range columnTypes {
		columnParsers[i] = getDuckDBConverter(columnType)
	}
	return &CSVFormatReader{
		columns:       columnNames,
		columnParsers: columnParsers,
		reader:        r,
	}, nil
}
func newCSVFormatReader(columnNames, columnTypes []string, reader io.Reader) (ClickhouseFormatReader, error) {
	return newCSVFormatReaderGeneric(columnNames, columnTypes, reader, ',', false)
}

func newCSVHeaderFormatReader(columnNames, columnTypes []string, reader io.Reader) (ClickhouseFormatReader, error) {
	return newCSVFormatReaderGeneric(columnNames, columnTypes, reader, ',', true)
}
func newTSVFormatReader(columnNames, columnTypes []string, reader io.Reader) (ClickhouseFormatReader, error) {
	return newCSVFormatReaderGeneric(columnNames, columnTypes, reader, '\t', false)
}
func newTSVHeaderFormatReader(columnNames, columnTypes []string, reader io.Reader) (ClickhouseFormatReader, error) {
	return newCSVFormatReaderGeneric(columnNames, columnTypes, reader, '\t', true)
}

type CSVFormatReader struct {
	columns       []string
	columnParsers []func(string) (driver.Value, error)
	reader        *csv.Reader
	closer        io.Closer
}

func (c *CSVFormatReader) Read(values []driver.Value) error {
	if len(c.columns) != len(values) {
		return errors.New("column length mismatch")
	}
	record, err := c.reader.Read()
	if err != nil {
		return err
	}
	for i := range c.columns {
		values[i], err = c.columnParsers[i](record[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *CSVFormatReader) Close() error {
	return c.closer.Close()
}

type CSVFormatWriter struct {
	columns []string
	writer  *csv.Writer
	closer  io.Closer
}

func (c *CSVFormatWriter) Write(values []any) error {
	strValues := make([]string, len(values))
	for i, value := range values {
		strValues[i] = duckValueToString(value)
	}
	return c.writer.Write(strValues)
}

func (c *CSVFormatWriter) Close() error {
	c.writer.Flush()
	return nil
}

var typesMapping = map[string]string{
	"INTEGER": "Int32",
	"VARCHAR": "String",
	"BIGINT":  "Int64",
	"BOOLEAN": "UInt8",
	"DOUBLE":  "Float64",
}

func typesToClickhouseTypes(types []string) []string {
	clickhouseTypes := make([]string, len(types))
	for i, t := range types {
		clickhouseTypes[i] = typesMapping[t]
		if clickhouseTypes[i] == "" {
			clickhouseTypes[i] = "String"
		}
	}
	return clickhouseTypes
}

func newCSVFormatWriterGeneric(columnNames, columnTypes []string, writer io.Writer, sep rune, header bool, types bool) (ClickhouseFormatWriter, error) {
	w := csv.NewWriter(writer)
	w.Comma = sep
	if header {
		err := w.Write(columnNames)
		if err != nil {
			return nil, err
		}
	}
	if types {

		err := w.Write(typesToClickhouseTypes(columnTypes))
		if err != nil {
			return nil, err
		}
	}
	return &CSVFormatWriter{
		columns: columnNames,
		writer:  w,
	}, nil
}

func newCSVFormatWriter(columnNames, columnTypes []string, writer io.Writer) (ClickhouseFormatWriter, error) {
	return newCSVFormatWriterGeneric(columnNames, columnTypes, writer, ',', false, false)
}

func newCSVHeaderFormatWriter(columnNames, columnTypes []string, writer io.Writer) (ClickhouseFormatWriter, error) {
	return newCSVFormatWriterGeneric(columnNames, columnTypes, writer, ',', true, false)
}

func newTSVFormatWriter(columnNames, columnTypes []string, writer io.Writer) (ClickhouseFormatWriter, error) {
	return newCSVFormatWriterGeneric(columnNames, columnTypes, writer, '\t', false, false)
}

func newTSVHeaderFormatWriter(columnNames, columnTypes []string, writer io.Writer) (ClickhouseFormatWriter, error) {
	return newCSVFormatWriterGeneric(columnNames, columnTypes, writer, '\t', true, false)
}

func newTSVHeaderWithTypesFormatWriter(columnNames, columnTypes []string, writer io.Writer) (ClickhouseFormatWriter, error) {
	return newCSVFormatWriterGeneric(columnNames, columnTypes, writer, '\t', true, true)
}

var chInputFormats = map[string]ClickhouseFormatReaderFactory{
	"JSONEachRow":           newJsonLinesFormatReader,
	"CSV":                   newCSVFormatReader,
	"CSVWithNames":          newCSVHeaderFormatReader,
	"TabSeparated":          newTSVFormatReader,
	"TabSeparatedWithNames": newTSVHeaderFormatReader,
}

var chOutputFormats = map[string]ClickhouseFormatWriterFactory{
	"JSONEachRow":                   newJsonLinesFormatWriter,
	"CSV":                           newCSVFormatWriter,
	"CSVWithNames":                  newCSVHeaderFormatWriter,
	"TabSeparated":                  newTSVFormatWriter,
	"TabSeparatedWithNames":         newTSVHeaderFormatWriter,
	"TabSeparatedWithNamesAndTypes": newTSVHeaderWithTypesFormatWriter,
}

var chFormatContentTypes = map[string]string{
	"TabSeparated":                  "text/tab-separated-values; charset=UTF-8",
	"TabSeparatedWithNames":         "text/tab-separated-values; charset=UTF-8",
	"TabSeparatedWithNamesAndTypes": "text/tab-separated-values; charset=UTF-8",
	"CSV":                           "text/csv; charset=UTF-8",
	"CSVWithNames":                  "text/csv; charset=UTF-8",
	"JSONEachRow":                   "application/json; charset=UTF-8",
}

func GetClickhouseFormatContentType(name string) string {
	return chFormatContentTypes[name]
}

func GetClickhouseInputFormat(name string) ClickhouseFormatReaderFactory {
	return chInputFormats[name]
}

func GetClickhouseOutputFormat(name string) ClickhouseFormatWriterFactory {
	return chOutputFormats[name]
}
