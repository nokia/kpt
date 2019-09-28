// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kio

import (
	"fmt"
	"os"
	"path/filepath"

	"lib.kpt.dev/kio/kioutil"
	"lib.kpt.dev/yaml"
)

// requiredResourcePackageAnnotations are annotations that are required to write resources back to
// files.
var requiredResourcePackageAnnotations = []string{
	kioutil.IndexAnnotation, kioutil.ModeAnnotation, kioutil.PathAnnotation,
}

type LocalPackageReadWriter struct {
	Kind string `yaml:"kind,omitempty"`

	// PackagePath is the path to the package directory.
	PackagePath string `yaml:"path,omitempty"`

	// MatchFilesGlob configures Read to only read Resources from files matching any of the
	// provided patterns.
	// Defaults to ["*.yaml", "*.yml"] if empty.  To match all files specify ["*"].
	MatchFilesGlob []string `yaml:"matchFilesGlob,omitempty"`

	// IncludeSubpackages will configure Read to read Resources from subpackages.
	// Subpackages are identified by having a Kptfile.
	IncludeSubpackages bool `yaml:"includeSubpackages,omitempty"`

	// ErrorIfNonResources will configure Read to throw an error if yaml missing missing
	// apiVersion or kind is read.
	ErrorIfNonResources bool `yaml:"errorIfNonResources,omitempty"`

	// OmitReaderAnnotations will cause the reader to skip annotating Resources with the file
	// path and mode.
	OmitReaderAnnotations bool `yaml:"omitReaderAnnotations,omitempty"`

	// SetAnnotations are annotations to set on the Resources as they are read.
	SetAnnotations map[string]string `yaml:"setAnnotations,omitempty"`
}

type PackageBuffer struct {
	Nodes []*yaml.RNode
}

func (r *PackageBuffer) Read() ([]*yaml.RNode, error) {
	return r.Nodes, nil
}

func (r *PackageBuffer) Write(nodes []*yaml.RNode) error {
	r.Nodes = nodes
	return nil
}

func (r LocalPackageReadWriter) Read() ([]*yaml.RNode, error) {
	return LocalPackageReader{
		PackagePath:         r.PackagePath,
		MatchFilesGlob:      r.MatchFilesGlob,
		IncludeSubpackages:  r.IncludeSubpackages,
		ErrorIfNonResources: r.ErrorIfNonResources,
		SetAnnotations:      r.SetAnnotations,
	}.Read()
}

func (r LocalPackageReadWriter) Write(nodes []*yaml.RNode) error {
	var clear []string
	for k := range r.SetAnnotations {
		clear = append(clear, k)
	}
	return LocalPackageWriter{
		PackagePath:      r.PackagePath,
		ClearAnnotations: clear,
	}.Write(nodes)
}

// LocalPackageReader reads ResourceNodes from a local package.
type LocalPackageReader struct {
	Kind string `yaml:"kind,omitempty"`

	// PackagePath is the path to the package directory.
	PackagePath string `yaml:"path,omitempty"`

	// MatchFilesGlob configures Read to only read Resources from files matching any of the
	// provided patterns.
	// Defaults to ["*.yaml", "*.yml"] if empty.  To match all files specify ["*"].
	MatchFilesGlob []string `yaml:"matchFilesGlob,omitempty"`

	// IncludeSubpackages will configure Read to read Resources from subpackages.
	// Subpackages are identified by having a Kptfile.
	IncludeSubpackages bool `yaml:"includeSubpackages,omitempty"`

	// ErrorIfNonResources will configure Read to throw an error if yaml missing missing
	// apiVersion or kind is read.
	ErrorIfNonResources bool `yaml:"errorIfNonResources,omitempty"`

	// OmitReaderAnnotations will cause the reader to skip annotating Resources with the file
	// path and mode.
	OmitReaderAnnotations bool `yaml:"omitReaderAnnotations,omitempty"`

	// SetAnnotations are annotations to set on the Resources as they are read.
	SetAnnotations map[string]string `yaml:"setAnnotations,omitempty"`
}

var _ Reader = LocalPackageReader{}

const kptFileName = "Kptfile"

var defaultMatch = []string{"*.yaml", "*.yml"}

// Read reads the Resources.
func (r LocalPackageReader) Read() ([]*yaml.RNode, error) {
	if r.PackagePath == "" {
		return nil, fmt.Errorf("must specify package path")
	}
	if len(r.MatchFilesGlob) == 0 {
		r.MatchFilesGlob = defaultMatch
	}

	var operand ResourceNodeSlice
	var pathRelativeTo string
	r.PackagePath = filepath.Clean(r.PackagePath)
	err := filepath.Walk(r.PackagePath, func(
		path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// is this the user specified path?
		if path == r.PackagePath {
			if info.IsDir() {
				// skip the root package directory
				pathRelativeTo = r.PackagePath
				return nil
			}

			// user specified path is a file rather than a directory.
			// make its path relative to its parent so it can be written to another file.
			pathRelativeTo = filepath.Dir(r.PackagePath)
		}

		// check if we should skip the directory or file
		if info.IsDir() {
			return r.shouldSkipDir(path, info)
		}
		if match, err := r.shouldSkipFile(path, info); err != nil {
			return err
		} else if !match {
			// skip this file
			return nil
		}

		// get the relative path to file within the package so we can write the files back out
		// to another location.
		path, err = filepath.Rel(pathRelativeTo, path)
		if err != nil {
			return err
		}

		r.initReaderAnnotations(path, info)
		nodes, err := r.readFile(filepath.Join(pathRelativeTo, path), info)
		if err != nil {
			return err
		}
		operand = append(operand, nodes...)
		return nil
	})
	return operand, err
}

// readFile reads the ResourceNodes from a file
func (r *LocalPackageReader) readFile(path string, info os.FileInfo) ([]*yaml.RNode, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rr := ByteReader{
		Reader:                f,
		OmitReaderAnnotations: r.OmitReaderAnnotations,
		SetAnnotations:        r.SetAnnotations,
	}
	return rr.Read()
}

// shouldSkipFile returns true if the file should be skipped
func (r *LocalPackageReader) shouldSkipFile(path string, info os.FileInfo) (bool, error) {
	// check if the files are in scope
	for _, g := range r.MatchFilesGlob {
		if match, err := filepath.Match(g, info.Name()); err != nil {
			return false, err
		} else if match {
			return true, nil
		}
	}
	return false, nil
}

// initReaderAnnotations adds the LocalPackageReader Annotations to r.SetAnnotations
func (r *LocalPackageReader) initReaderAnnotations(path string, info os.FileInfo) {
	if r.SetAnnotations == nil {
		r.SetAnnotations = map[string]string{}
	}
	if !r.OmitReaderAnnotations {
		r.SetAnnotations[kioutil.PackageAnnotation] = filepath.Dir(path)
		r.SetAnnotations[kioutil.PathAnnotation] = path
		r.SetAnnotations[kioutil.ModeAnnotation] = fmt.Sprintf("%d", info.Mode())
	}
}

// shouldSkipDir returns a filepath.SkipDir if the directory should be skipped
func (r *LocalPackageReader) shouldSkipDir(path string, info os.FileInfo) error {
	// check if this is a subpackage
	_, err := os.Stat(filepath.Join(path, kptFileName))
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	if !r.IncludeSubpackages {
		return filepath.SkipDir
	}
	return nil
}
