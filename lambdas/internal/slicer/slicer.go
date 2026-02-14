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
func DefaultOptions() Options {
	return Options{
		DarknessThreshold: 128,
		DilationRadius:    15,
		MinGapHeight:      10,
		MinSliceHeight:    40,
		Padding:           15,
		JPEGQuality:       85,
	}
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

	// Step 1: Compute vertical projection profile — count dark pixels per row.
	profile := projectionProfile(img, bounds, opts.DarknessThreshold)

	// Step 2: Dilate profile to merge nearby text lines.
	dilated := dilateProfile(profile, opts.DilationRadius)

	// Step 3: Find gap regions (contiguous runs of zero in dilated profile).
	regions := findRegions(dilated, height, opts.MinGapHeight)

	// If fewer than 2 regions, return the full image as one slice.
	if len(regions) < 2 {
		data, err := encodeJPEG(img, bounds, opts.JPEGQuality)
		if err != nil {
			return nil, fmt.Errorf("encode full image: %w", err)
		}
		return []Slice{{Index: 0, ImageData: data, Y0: 0, Y1: height}}, nil
	}

	// Step 4: Crop each region with padding and encode as JPEG.
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

// findRegions identifies contiguous non-zero runs in the dilated profile.
// Returns a list of [y0, y1) pairs. Gaps shorter than minGapHeight between
// non-zero regions are merged (the regions on either side are combined).
func findRegions(dilated []int, height, minGapHeight int) [][2]int {
	var regions [][2]int
	inRegion := false
	start := 0

	for y := 0; y < height; y++ {
		if dilated[y] > 0 {
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
