package slicer

import (
	"bytes"
	"fmt"
	"image"
	"image/draw"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"io"
	"log"
	"os"
	"os/exec"

	_ "golang.org/x/image/bmp"
	_ "golang.org/x/image/tiff"
	_ "golang.org/x/image/webp"
)

// Options controls the slicing algorithm.
type Options struct {
	DarknessThreshold uint8 // Luma below this = "dark" (default: 128)
	DilationRadius    int   // Rows to smear +/- (default: 15)
	MinGapHeight      int   // Min gap rows to split (default: 10)
	MinSliceHeight    int   // Discard tiny slices (default: 40)
	Padding           int   // Extra rows above/below cut (default: 15)
	JPEGQuality       int   // Output quality (default: 85)
}

// Slice represents a cropped strip of the original image.
type Slice struct {
	Index     int
	ImageData []byte // JPEG-encoded
	Y0, Y1    int    // Crop coords in original
}

// DefaultOptions returns sensible defaults for logbook page slicing.
// Spatial parameters (DilationRadius, MinGapHeight, MinSliceHeight, Padding)
// are calibrated for a reference height of 3024 pixels (iPhone portrait photo)
// and are automatically scaled to the actual image height by SliceImage.
func DefaultOptions() Options {
	return Options{
		DarknessThreshold: 128,
		DilationRadius:    80,
		MinGapHeight:      10,
		MinSliceHeight:    150,
		Padding:           15,
		JPEGQuality:       85,
	}
}

// referenceHeight is the image height that DefaultOptions spatial parameters
// are calibrated against (iPhone portrait photo at ~4032x3024).
const referenceHeight = 3024

// scaleToHeight returns a copy of opts with spatial parameters scaled
// proportionally from referenceHeight to the actual image height.
func scaleToHeight(opts Options, height int) Options {
	if height <= 0 || height == referenceHeight {
		return opts
	}
	scale := func(v int) int {
		s := v * height / referenceHeight
		if s < 1 && v > 0 {
			return 1
		}
		return s
	}
	opts.DilationRadius = scale(opts.DilationRadius)
	opts.MinGapHeight = scale(opts.MinGapHeight)
	opts.MinSliceHeight = scale(opts.MinSliceHeight)
	opts.Padding = scale(opts.Padding)
	return opts
}

// SliceImage decodes an image and splits it into horizontal strips at blank
// gaps between entries. If fewer than 2 regions are detected, the full image
// is returned as a single slice.
//
// Supports JPEG, PNG, GIF, BMP, TIFF, and WebP natively. For HEIC/HEIF and
// other formats not decodable by Go, it attempts conversion to JPEG via
// external tools (sips on macOS, magick/convert on Linux).
func SliceImage(imageBytes []byte, opts Options) ([]Slice, error) {
	img, _, err := image.Decode(bytes.NewReader(imageBytes))
	if err != nil {
		// Native decode failed — try converting via external tool.
		converted, convErr := convertToJPEG(imageBytes)
		if convErr != nil {
			return nil, fmt.Errorf("decode image: %w (conversion also failed: %v)", err, convErr)
		}
		img, _, err = image.Decode(bytes.NewReader(converted))
		if err != nil {
			return nil, fmt.Errorf("decode converted image: %w", err)
		}
		log.Printf("slicer: converted non-native image format to JPEG (%d → %d bytes)", len(imageBytes), len(converted))
	}

	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()

	// Scale spatial parameters to the actual image height so the algorithm
	// works consistently across different resolutions (phone cameras, scanners, etc).
	opts = scaleToHeight(opts, height)

	// Step 1: Compute vertical projection profile — count dark pixels per row.
	profile := projectionProfile(img, bounds, opts.DarknessThreshold)

	// Step 2: Subtract noise floor. Real-world photos of logbooks always have
	// dark pixels from table grid lines, binding shadows, and sensor noise.
	// We use 7% of image width as the floor: this zeroes out both pure
	// background noise (2-4% of width) and empty table rows with vertical
	// grid lines (5-7% of width). Only actual text content (8%+) survives.
	noiseFloor := width * 7 / 100
	for i, v := range profile {
		if v > noiseFloor {
			profile[i] = v - noiseFloor
		} else {
			profile[i] = 0
		}
	}

	// Step 3: Smooth profile with a moving average. Unlike max-dilation,
	// a moving average naturally distinguishes narrow within-entry gaps
	// (which stay non-zero because surrounding content contributes to the
	// average) from wide between-entry gaps (which average to zero because
	// all rows in the window are empty).
	smoothed := smoothProfile(profile, opts.DilationRadius)

	// Step 4: Apply content threshold. After the aggressive noise floor and
	// smoothing, remaining non-zero values represent genuine text content.
	// A low threshold catches weak content (like aircraft info headers) that
	// the smoothing reduces in amplitude.
	contentThreshold := width / 100 // ~1% of width

	// Step 5: Find gap regions (contiguous runs below threshold in smoothed profile).
	regions := findRegions(smoothed, height, opts.MinGapHeight, contentThreshold)

	// Step 6: Absorb small regions into neighbors. Logbook entries have an
	// aircraft info header above the entry text, sometimes separated by a gap
	// wider than the gap between consecutive entries. This merges orphaned
	// aircraft info sections and tiny fragments back into the nearest entry.
	minEntryHeight := height / 8
	regions = absorbSmallRegions(regions, minEntryHeight)

	// If fewer than 2 regions, return the full image as one slice.
	if len(regions) < 2 {
		data, err := encodeJPEG(img, bounds, opts.JPEGQuality)
		if err != nil {
			return nil, fmt.Errorf("encode full image: %w", err)
		}
		return []Slice{{Index: 0, ImageData: data, Y0: 0, Y1: height}}, nil
	}

	// Step 7: Crop each region with padding and encode as JPEG.
	var slices []Slice
	idx := 0
	for _, r := range regions {
		y0 := r[0] - opts.Padding
		y1 := r[1] + opts.Padding
		if y0 < 0 {
			y0 = 0
		}
		if y1 > height {
			y1 = height
		}

		if y1-y0 < opts.MinSliceHeight {
			continue
		}

		cropRect := image.Rect(bounds.Min.X, bounds.Min.Y+y0, bounds.Min.X+width, bounds.Min.Y+y1)
		data, err := encodeJPEG(img, cropRect, opts.JPEGQuality)
		if err != nil {
			return nil, fmt.Errorf("encode slice %d: %w", idx, err)
		}
		slices = append(slices, Slice{Index: idx, ImageData: data, Y0: y0, Y1: y1})
		idx++
	}

	if len(slices) == 0 {
		data, err := encodeJPEG(img, bounds, opts.JPEGQuality)
		if err != nil {
			return nil, fmt.Errorf("encode full image: %w", err)
		}
		return []Slice{{Index: 0, ImageData: data, Y0: 0, Y1: height}}, nil
	}

	return slices, nil
}

// convertToJPEG attempts to convert image bytes to JPEG using external tools.
// Tries sips (macOS) first, then magick (ImageMagick 7), then convert (ImageMagick 6).
func convertToJPEG(imageBytes []byte) ([]byte, error) {
	converters := []struct {
		name string
		args func(inPath, outPath string) []string
	}{
		{"sips", func(in, out string) []string {
			return []string{"-s", "format", "jpeg", in, "--out", out}
		}},
		{"magick", func(in, out string) []string {
			return []string{in, out}
		}},
		{"convert", func(in, out string) []string {
			return []string{in, out}
		}},
	}

	for _, conv := range converters {
		path, err := exec.LookPath(conv.name)
		if err != nil {
			continue
		}

		result, err := runConverter(path, conv.args, imageBytes)
		if err != nil {
			log.Printf("slicer: %s conversion failed: %v", conv.name, err)
			continue
		}
		return result, nil
	}

	return nil, fmt.Errorf("no image converter available (tried sips, magick, convert)")
}

// runConverter writes input to a temp file, runs the converter, reads the output.
func runConverter(bin string, argsFn func(string, string) []string, imageBytes []byte) ([]byte, error) {
	inFile, err := os.CreateTemp("", "slicer-in-*")
	if err != nil {
		return nil, fmt.Errorf("create temp input: %w", err)
	}
	defer os.Remove(inFile.Name())

	if _, err := inFile.Write(imageBytes); err != nil {
		inFile.Close()
		return nil, fmt.Errorf("write temp input: %w", err)
	}
	inFile.Close()

	outPath := inFile.Name() + ".jpg"
	defer os.Remove(outPath)

	args := argsFn(inFile.Name(), outPath)
	cmd := exec.Command(bin, args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("%s: %w (%s)", bin, err, string(output))
	}

	outFile, err := os.Open(outPath)
	if err != nil {
		return nil, fmt.Errorf("open converted output: %w", err)
	}
	defer outFile.Close()

	return io.ReadAll(outFile)
}

// projectionProfile counts dark pixels per row.
func projectionProfile(img image.Image, bounds image.Rectangle, threshold uint8) []int {
	height := bounds.Dy()
	width := bounds.Dx()
	profile := make([]int, height)

	for y := 0; y < height; y++ {
		count := 0
		for x := 0; x < width; x++ {
			r, g, b, _ := img.At(bounds.Min.X+x, bounds.Min.Y+y).RGBA()
			// Luma using BT.601 coefficients (values are 16-bit, shift to 8-bit).
			luma := uint8((19595*(r>>8) + 38470*(g>>8) + 7471*(b>>8) + 1<<15) >> 16)
			if luma < threshold {
				count++
			}
		}
		profile[y] = count
	}
	return profile
}

// smoothProfile applies a moving average with the given radius.
// Each output value is the mean of input values in [i-radius, i+radius].
// This bridges narrow gaps surrounded by content while preserving wide gaps.
func smoothProfile(profile []int, radius int) []int {
	n := len(profile)
	if n == 0 {
		return profile
	}

	// Build prefix sum for O(n) moving average
	prefix := make([]int, n+1)
	for i, v := range profile {
		prefix[i+1] = prefix[i] + v
	}

	smoothed := make([]int, n)
	for i := range n {
		lo := i - radius
		hi := i + radius
		if lo < 0 {
			lo = 0
		}
		if hi >= n {
			hi = n - 1
		}
		width := hi - lo + 1
		smoothed[i] = (prefix[hi+1] - prefix[lo]) / width
	}
	return smoothed
}

// dilateProfile applies a sliding-window max to spread non-zero values.
func dilateProfile(profile []int, radius int) []int {
	n := len(profile)
	dilated := make([]int, n)
	for i := 0; i < n; i++ {
		lo := i - radius
		hi := i + radius
		if lo < 0 {
			lo = 0
		}
		if hi >= n {
			hi = n - 1
		}
		maxVal := 0
		for j := lo; j <= hi; j++ {
			if profile[j] > maxVal {
				maxVal = profile[j]
			}
		}
		dilated[i] = maxVal
	}
	return dilated
}

// findRegions identifies contiguous content runs in the profile.
// A row is considered content if its value exceeds contentThreshold.
// Returns a list of [y0, y1) pairs. Gaps shorter than minGapHeight between
// content regions are merged (the regions on either side are combined).
func findRegions(dilated []int, height, minGapHeight, contentThreshold int) [][2]int {
	var regions [][2]int
	inRegion := false
	start := 0

	for y := 0; y < height; y++ {
		if dilated[y] > contentThreshold {
			if !inRegion {
				inRegion = true
				start = y
			}
		} else {
			if inRegion {
				inRegion = false
				regions = append(regions, [2]int{start, y})
			}
		}
	}
	if inRegion {
		regions = append(regions, [2]int{start, height})
	}

	// Merge regions separated by gaps smaller than minGapHeight.
	merged := mergeRegions(regions, minGapHeight)
	return merged
}

// absorbSmallRegions iteratively merges regions that are too small to be a
// standalone logbook entry into their nearest neighbor (smallest gap).
// This handles orphaned aircraft info headers and small fragments that are
// visually part of an adjacent entry but separated by a gap.
func absorbSmallRegions(regions [][2]int, minHeight int) [][2]int {
	for {
		// Find smallest region below threshold.
		smallIdx := -1
		smallHeight := 0
		for i, r := range regions {
			h := r[1] - r[0]
			if h < minHeight && (smallIdx == -1 || h < smallHeight) {
				smallIdx = i
				smallHeight = h
			}
		}
		if smallIdx == -1 {
			break // All regions are large enough.
		}
		if len(regions) < 2 {
			break // Nothing to merge with.
		}

		// Find adjacent region with smallest gap.
		mergeWith := -1
		bestGap := 0
		if smallIdx > 0 {
			gap := regions[smallIdx][0] - regions[smallIdx-1][1]
			mergeWith = smallIdx - 1
			bestGap = gap
		}
		if smallIdx < len(regions)-1 {
			gap := regions[smallIdx+1][0] - regions[smallIdx][1]
			if mergeWith == -1 || gap < bestGap {
				mergeWith = smallIdx + 1
			}
		}

		// Merge: expand the neighbor to cover both regions.
		lo, hi := smallIdx, mergeWith
		if lo > hi {
			lo, hi = hi, lo
		}
		merged := [2]int{regions[lo][0], regions[hi][1]}
		regions = append(regions[:lo], append([][2]int{merged}, regions[hi+1:]...)...)
	}
	return regions
}

// mergeRegions combines adjacent regions whose gap is smaller than minGap.
func mergeRegions(regions [][2]int, minGap int) [][2]int {
	if len(regions) <= 1 {
		return regions
	}
	merged := [][2]int{regions[0]}
	for i := 1; i < len(regions); i++ {
		last := &merged[len(merged)-1]
		gap := regions[i][0] - last[1]
		if gap < minGap {
			last[1] = regions[i][1]
		} else {
			merged = append(merged, regions[i])
		}
	}
	return merged
}

// encodeJPEG crops the image to the given rectangle and encodes as JPEG.
func encodeJPEG(img image.Image, rect image.Rectangle, quality int) ([]byte, error) {
	cropped := image.NewRGBA(image.Rect(0, 0, rect.Dx(), rect.Dy()))
	draw.Draw(cropped, cropped.Bounds(), img, rect.Min, draw.Src)

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, cropped, &jpeg.Options{Quality: quality}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
