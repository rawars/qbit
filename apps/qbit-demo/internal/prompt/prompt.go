// Package prompt provides the small interactive setup used by qbit-demo.
package prompt

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Reader reads values while showing safe defaults.
type Reader struct {
	input  *bufio.Reader
	output io.Writer
}

// New creates an interactive prompt reader.
func New(input io.Reader, output io.Writer) *Reader {
	return &Reader{input: bufio.NewReader(input), output: output}
}

// String asks for a string.
func (reader *Reader) String(label, defaultValue string) (string, error) {
	fmt.Fprintf(reader.output, "%s [%s]: ", label, defaultValue)
	value, err := reader.input.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	value = strings.TrimSpace(value)
	if value == "" {
		value = defaultValue
	}
	return value, nil
}

// Int asks for an integer.
func (reader *Reader) Int(label string, defaultValue int) (int, error) {
	value, err := reader.String(label, strconv.Itoa(defaultValue))
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", label)
	}
	return parsed, nil
}

// Int64 asks for a 64-bit integer.
func (reader *Reader) Int64(label string, defaultValue int64) (int64, error) {
	value, err := reader.String(label, strconv.FormatInt(defaultValue, 10))
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", label)
	}
	return parsed, nil
}

// Float asks for a floating point value.
func (reader *Reader) Float(label string, defaultValue float64) (float64, error) {
	value, err := reader.String(label, strconv.FormatFloat(defaultValue, 'f', -1, 64))
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a number", label)
	}
	return parsed, nil
}
