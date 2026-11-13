package config

import (
	"errors"
	"fmt"
	htmltemplate "html/template"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
	texttemplate "text/template"
)

// Error page formats
const (
	ErrorFormatHTML = "html"
	ErrorFormatJSON = "json"
	ErrorFormatText = "text"
)

// ErrorPages is how ntlb answers what it does not proxy: blocks, limits, no healthy upstream, a
// failed upstream, a drain. Answers from upstreams pass through as they are.
type ErrorPages struct {
	Format   string `yaml:"format"`   // html (default), json or text
	Template string `yaml:"template"` // A Go template file instead of a format; HTML-escaped if .html/.htm

	ContentType string   `yaml:"-"` // Of the template, by its extension
	Executor    Executor `yaml:"-"` // The parsed template; nil for a format
}

// Executor is a parsed text/template or html/template
type Executor interface {
	Execute(w io.Writer, data any) error
}

// applyDefaults sets the default format; nil stays nil (the global settings, or the default)
func (e *ErrorPages) applyDefaults() {
	if e != nil && e.Format == "" && e.Template == "" {
		e.Format = ErrorFormatHTML
	}
}

// validate checks the format, or parses the template
func (e *ErrorPages) validate() error {
	if e == nil {
		return nil
	} else if e.Format != "" && e.Template != "" {
		return errors.New("errors: format and template are exclusive")
	} else if e.Template != "" {
		return e.parseTemplate()
	}
	switch e.Format {
	case ErrorFormatHTML, ErrorFormatJSON, ErrorFormatText:
		return nil
	default:
		return fmt.Errorf("errors.format: unknown format %q, expected html, json or text", e.Format)
	}
}

// parseTemplate reads and parses the template: html/template for HTML files, so that its values
// are escaped, else text/template
func (e *ErrorPages) parseTemplate() error {
	b, err := os.ReadFile(e.Template)
	if err != nil {
		return fmt.Errorf("errors.template: %w", err)
	}
	ext := strings.ToLower(filepath.Ext(e.Template))
	if ext == ".html" || ext == ".htm" {
		e.Executor, err = htmltemplate.New("errors").Parse(string(b))
	} else {
		e.Executor, err = texttemplate.New("errors").Parse(string(b))
	}
	if err != nil {
		return fmt.Errorf("errors.template: %w", err)
	}
	e.ContentType = mime.TypeByExtension(ext)
	if e.ContentType == "" {
		e.ContentType = "text/plain; charset=utf-8"
	}
	return nil
}
