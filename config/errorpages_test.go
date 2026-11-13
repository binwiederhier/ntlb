package config

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParse_ErrorPages(t *testing.T) {
	// Global error pages, replaced by a site's own; HTML without any
	c, err := Parse([]byte(`
errors: {format: json}
sites:
  - {name: a, listen-http: ':80', hosts: [a.com], errors: {format: text}, ` + testUp + `}
  - {name: b, listen-http: ':80', ` + testUp + `}
`))
	require.NoError(t, err)
	require.Equal(t, ErrorFormatText, c.ErrorPagesFor(c.Sites[0]).Format)
	require.Equal(t, ErrorFormatJSON, c.ErrorPagesFor(c.Sites[1]).Format)
	require.Equal(t, ErrorFormatJSON, c.ErrorPagesFor(nil).Format)

	c, err = Parse([]byte("sites: [{name: a, listen-http: ':80', " + testUp + "}]"))
	require.NoError(t, err)
	require.Equal(t, ErrorFormatHTML, c.ErrorPagesFor(c.Sites[0]).Format)
	require.Equal(t, ErrorFormatHTML, c.ErrorPagesFor(nil).Format)

	// A format left out is HTML
	c, err = Parse([]byte("sites: [{name: a, listen-http: ':80', errors: {}, " + testUp + "}]"))
	require.NoError(t, err)
	require.Equal(t, ErrorFormatHTML, c.ErrorPagesFor(c.Sites[0]).Format)
}

func TestParse_ErrorPagesTemplate(t *testing.T) {
	dir := t.TempDir()
	html := filepath.Join(dir, "error.html")
	require.NoError(t, os.WriteFile(html, []byte("<p>{{.Error}}</p>"), 0600))
	jsonFile := filepath.Join(dir, "error.json")
	require.NoError(t, os.WriteFile(jsonFile, []byte(`{"e":"{{.Error}}"}`), 0600))

	// HTML templates escape, others do not; the content type comes from the extension
	c, err := Parse([]byte("errors: {template: " + html + "}\nsites: [{name: a, listen-http: ':80', errors: {template: " + jsonFile + "}, " + testUp + "}]"))
	require.NoError(t, err)
	pages := c.ErrorPagesFor(nil)
	require.Equal(t, "text/html; charset=utf-8", pages.ContentType)
	var b bytes.Buffer
	require.NoError(t, pages.Executor.Execute(&b, map[string]string{"Error": "<x>"}))
	require.Equal(t, "<p>&lt;x&gt;</p>", b.String())
	pages = c.ErrorPagesFor(c.Sites[0])
	require.Equal(t, "application/json", pages.ContentType)
	b.Reset()
	require.NoError(t, pages.Executor.Execute(&b, map[string]string{"Error": "<x>"}))
	require.Equal(t, `{"e":"<x>"}`, b.String())

	broken := filepath.Join(dir, "broken.html")
	require.NoError(t, os.WriteFile(broken, []byte("{{.Error"), 0600))
	for errors, want := range map[string]string{
		"{format: xml}":                          `errors.format: unknown format "xml", expected html, json or text`,
		"{format: json, template: " + html + "}": "errors: format and template are exclusive",
		"{template: " + dir + "/missing.html}":   "errors.template: open",
		"{template: " + broken + "}":             "errors.template: template:",
	} {
		_, err := Parse([]byte("errors: " + errors + "\nsites: [{name: a, listen-http: ':80', " + testUp + "}]"))
		require.ErrorContains(t, err, want, errors)
		_, err = Parse([]byte("sites: [{name: a, listen-http: ':80', errors: " + errors + ", " + testUp + "}]"))
		require.ErrorContains(t, err, "site a: "+want, errors)
	}
}
