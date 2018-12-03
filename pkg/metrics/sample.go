// Copyright 2018 vip.com.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	gometrics "github.com/rcrowley/go-metrics"
	"sync"
)

type UniformSample struct {
	count         int64
	mutex         sync.Mutex
	reservoirSize int
	values        []int64
}

// NewUniformSample constructs a new uniform sample with the given reservoir
// size.
func NewUniformSample(reservoirSize int) gometrics.Sample {
	return &UniformSample{
		reservoirSize: reservoirSize,
		values:        make([]int64, 0, reservoirSize),
	}
}

// Clear clears all samples.
func (s *UniformSample) Clear() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.count = 0
	s.values = make([]int64, 0, s.reservoirSize)
}

// Count returns the number of samples recorded, which may exceed the
// reservoir size.
func (s *UniformSample) Count() int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.count
}

// Max returns the maximum value in the sample, which may not be the maximum
// value ever to be part of the sample.
func (s *UniformSample) Max() int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return gometrics.SampleMax(s.values)
}

// Mean returns the mean of the values in the sample.
func (s *UniformSample) Mean() float64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return gometrics.SampleMean(s.values)
}

// Min returns the minimum value in the sample, which may not be the minimum
// value ever to be part of the sample.
func (s *UniformSample) Min() int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return gometrics.SampleMin(s.values)
}

// Percentile returns an arbitrary percentile of values in the sample.
func (s *UniformSample) Percentile(p float64) float64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return gometrics.SamplePercentile(s.values, p)
}

// Percentiles returns a slice of arbitrary percentiles of values in the
// sample.
func (s *UniformSample) Percentiles(ps []float64) []float64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return gometrics.SamplePercentiles(s.values, ps)
}

// Size returns the size of the sample, which is at most the reservoir size.
func (s *UniformSample) Size() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.values)
}

// Snapshot returns a read-only copy of the sample.
func (s *UniformSample) Snapshot() gometrics.Sample {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	values := make([]int64, len(s.values))
	copy(values, s.values)
	return gometrics.NewSampleSnapshot(s.count, values)
}

// StdDev returns the standard deviation of the values in the sample.
func (s *UniformSample) StdDev() float64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return gometrics.SampleStdDev(s.values)
}

// Sum returns the sum of the values in the sample.
func (s *UniformSample) Sum() int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return gometrics.SampleSum(s.values)
}

// Update samples a new value.
func (s *UniformSample) Update(v int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.count++
	if len(s.values) < s.reservoirSize {
		s.values = append(s.values, v)
	} else {
		// Use circle buffer to eliminate the oldest value
		idx := s.count % int64(s.reservoirSize)
		s.values[idx] = v
	}
}

// Values returns a copy of the values in the sample.
func (s *UniformSample) Values() []int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	values := make([]int64, len(s.values))
	copy(values, s.values)
	return values
}

// Variance returns the variance of the values in the sample.
func (s *UniformSample) Variance() float64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return gometrics.SampleVariance(s.values)
}
