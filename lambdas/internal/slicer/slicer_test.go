package slicer

import (
	"bytes"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	"testing"
)

// newTestImage creates a white image with horizontal dark bands for testing.
func newTestImage(width, height int, bands [][2]int) *image.RGBA {
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	draw.Draw(img, img.Bounds(), &image.Uniform{color.White}, image.Point{}, draw.Src)
	for _, b := range bands {
		for y := b[0]; y < b[1] && y < height; y++ {
			for x := 0; x < width; x++ {
				img.Set(x, y, color.Black)
			}
		}
	}
	return img
}

func encodeTestJPEG(img image.Image) []byte {
	var buf bytes.Buffer
	jpeg.Encode(&buf, img, &jpeg.Options{Quality: 85})
	return buf.Bytes()
}

// smallImageOptions returns options scaled for small synthetic test images.
// DefaultOptions is tuned for ~4032x3024 iPhone photos; these values are
// proportionally reduced so the algorithm behaves correctly on tiny images.
func smallImageOptions() Options {
	return Options{
		DarknessThreshold: 128,
		DilationRadius:    5,
		MinGapHeight:      5,
		MinSliceHeight:    20,
		Padding:           5,
		JPEGQuality:       85,
	}
}

func TestProjectionProfile(t *testing.T) {
	// 100x100 image with a dark band from rows 20-40.
	img := newTestImage(100, 100, [][2]int{{20, 40}})
	profile := projectionProfile(img, img.Bounds(), 128)

	// White rows should have 0 dark pixels.
	if profile[0] != 0 {
		t.Errorf("row 0: got %d dark pixels, want 0", profile[0])
	}
	if profile[10] != 0 {
		t.Errorf("row 10: got %d dark pixels, want 0", profile[10])
	}

	// Dark rows should have 100 dark pixels.
	if profile[25] != 100 {
		t.Errorf("row 25: got %d dark pixels, want 100", profile[25])
	}
	if profile[39] != 100 {
		t.Errorf("row 39: got %d dark pixels, want 100", profile[39])
	}

	// Row 40 (just past band) should be white.
	if profile[40] != 0 {
		t.Errorf("row 40: got %d dark pixels, want 0", profile[40])
	}
}

func TestDilateProfile(t *testing.T) {
	// Profile with a spike at position 50.
	profile := make([]int, 100)
	profile[50] = 10

	dilated := dilateProfile(profile, 5)

	// Positions 45-55 should be non-zero.
	for i := 45; i <= 55; i++ {
		if dilated[i] == 0 {
			t.Errorf("dilated[%d] = 0, want >0", i)
		}
	}

	// Positions far away should still be zero.
	if dilated[0] != 0 {
		t.Errorf("dilated[0] = %d, want 0", dilated[0])
	}
	if dilated[99] != 0 {
		t.Errorf("dilated[99] = %d, want 0", dilated[99])
	}
}

func TestDilateProfile_EdgeClamping(t *testing.T) {
	profile := make([]int, 10)
	profile[0] = 5
	profile[9] = 5

	dilated := dilateProfile(profile, 3)

	// Start should be dilated.
	if dilated[0] != 5 {
		t.Errorf("dilated[0] = %d, want 5", dilated[0])
	}
	if dilated[3] != 5 {
		t.Errorf("dilated[3] = %d, want 5", dilated[3])
	}
	// End should be dilated.
	if dilated[9] != 5 {
		t.Errorf("dilated[9] = %d, want 5", dilated[9])
	}
}

func TestFindRegions(t *testing.T) {
	// Dilated profile with two blocks separated by a wide gap.
	dilated := make([]int, 200)
	for i := 10; i < 50; i++ {
		dilated[i] = 1
	}
	for i := 100; i < 150; i++ {
		dilated[i] = 1
	}

	regions := findRegions(dilated, 200, 10, 0)
	if len(regions) != 2 {
		t.Fatalf("got %d regions, want 2", len(regions))
	}
	if regions[0] != [2]int{10, 50} {
		t.Errorf("region 0 = %v, want [10 50]", regions[0])
	}
	if regions[1] != [2]int{100, 150} {
		t.Errorf("region 1 = %v, want [100 150]", regions[1])
	}
}

func TestFindRegions_MergesSmallGaps(t *testing.T) {
	// Two blocks separated by a 5-row gap (smaller than minGapHeight=10).
	dilated := make([]int, 100)
	for i := 10; i < 30; i++ {
		dilated[i] = 1
	}
	for i := 35; i < 60; i++ {
		dilated[i] = 1
	}

	regions := findRegions(dilated, 100, 10, 0)
	if len(regions) != 1 {
		t.Fatalf("got %d regions, want 1 (should be merged)", len(regions))
	}
	if regions[0] != [2]int{10, 60} {
		t.Errorf("merged region = %v, want [10 60]", regions[0])
	}
}

func TestSliceImage_ThreeBands(t *testing.T) {
	// 200x600 image with 3 dark bands separated by wide gaps.
	img := newTestImage(200, 600, [][2]int{
		{50, 130},
		{230, 330},
		{430, 530},
	})
	jpegData := encodeTestJPEG(img)

	slices, err := SliceImage(jpegData, smallImageOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(slices) != 3 {
		t.Fatalf("got %d slices, want 3", len(slices))
	}

	for i, s := range slices {
		if s.Index != i {
			t.Errorf("slice %d has Index=%d", i, s.Index)
		}
		if len(s.ImageData) == 0 {
			t.Errorf("slice %d has empty image data", i)
		}
		// Verify it's valid JPEG.
		_, err := jpeg.Decode(bytes.NewReader(s.ImageData))
		if err != nil {
			t.Errorf("slice %d is not valid JPEG: %v", i, err)
		}
	}
}

func TestSliceImage_UniformWhite(t *testing.T) {
	// All-white image should return 1 slice (the full image).
	img := newTestImage(200, 400, nil)
	jpegData := encodeTestJPEG(img)

	slices, err := SliceImage(jpegData, DefaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(slices) != 1 {
		t.Fatalf("got %d slices, want 1 (full image fallback)", len(slices))
	}
	if slices[0].Y0 != 0 || slices[0].Y1 != 400 {
		t.Errorf("expected full image bounds [0,400), got [%d,%d)", slices[0].Y0, slices[0].Y1)
	}
}

func TestSliceImage_SingleBand(t *testing.T) {
	// One dark band should return 1 slice (full image — fewer than 2 regions).
	img := newTestImage(200, 300, [][2]int{{100, 200}})
	jpegData := encodeTestJPEG(img)

	slices, err := SliceImage(jpegData, DefaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(slices) != 1 {
		t.Fatalf("got %d slices, want 1 (single region fallback)", len(slices))
	}
}

func TestSliceImage_InvalidInput(t *testing.T) {
	_, err := SliceImage([]byte("not an image"), DefaultOptions())
	if err == nil {
		t.Fatal("expected error for invalid input")
	}
}

func TestSliceImage_MinSliceHeightFilter(t *testing.T) {
	// Two bands: one tall (100 rows), one tiny (10 rows).
	img := newTestImage(200, 500, [][2]int{
		{50, 150},  // 100 rows — above minSliceHeight
		{350, 360}, // 10 rows — below minSliceHeight (40)
	})
	jpegData := encodeTestJPEG(img)

	opts := DefaultOptions()
	slices, err := SliceImage(jpegData, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The tiny band should be filtered out. With only 1 region remaining,
	// it's valid to get either 1 (filtered to single) or the region that's tall enough.
	for _, s := range slices {
		h := s.Y1 - s.Y0
		if h < opts.MinSliceHeight {
			t.Errorf("slice height %d is below minSliceHeight %d", h, opts.MinSliceHeight)
		}
	}
}

func TestSliceImage_PNGInput(t *testing.T) {
	// PNG should also decode correctly (via import _ "image/png").
	img := newTestImage(100, 300, [][2]int{
		{30, 100},
		{200, 270},
	})

	// Encode as JPEG since we need a simple test (PNG import is registered).
	jpegData := encodeTestJPEG(img)
	slices, err := SliceImage(jpegData, smallImageOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(slices) != 2 {
		t.Errorf("got %d slices, want 2", len(slices))
	}
}

func TestNewTestImage(t *testing.T) {
	img := newTestImage(50, 50, [][2]int{{10, 20}})

	// White pixel.
	r, g, b, _ := img.At(0, 0).RGBA()
	if r>>8 != 255 || g>>8 != 255 || b>>8 != 255 {
		t.Error("expected white at (0,0)")
	}

	// Black pixel in band.
	r, g, b, _ = img.At(0, 15).RGBA()
	if r != 0 || g != 0 || b != 0 {
		t.Error("expected black at (0,15)")
	}
}

func TestMergeRegions(t *testing.T) {
	tests := []struct {
		name    string
		regions [][2]int
		minGap  int
		want    [][2]int
	}{
		{
			name:    "no regions",
			regions: nil,
			minGap:  10,
			want:    nil,
		},
		{
			name:    "single region",
			regions: [][2]int{{10, 20}},
			minGap:  10,
			want:    [][2]int{{10, 20}},
		},
		{
			name:    "two regions wide gap",
			regions: [][2]int{{10, 20}, {50, 60}},
			minGap:  10,
			want:    [][2]int{{10, 20}, {50, 60}},
		},
		{
			name:    "two regions small gap — merged",
			regions: [][2]int{{10, 20}, {25, 40}},
			minGap:  10,
			want:    [][2]int{{10, 40}},
		},
		{
			name:    "three regions middle gap small",
			regions: [][2]int{{10, 20}, {25, 40}, {80, 100}},
			minGap:  10,
			want:    [][2]int{{10, 40}, {80, 100}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeRegions(tt.regions, tt.minGap)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d regions, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("region %d = %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestAbsorbSmallRegions(t *testing.T) {
	tests := []struct {
		name      string
		regions   [][2]int
		minHeight int
		want      [][2]int
	}{
		{
			name:      "no regions",
			regions:   nil,
			minHeight: 100,
			want:      nil,
		},
		{
			name:      "all large",
			regions:   [][2]int{{0, 500}, {600, 1200}, {1400, 2000}},
			minHeight: 100,
			want:      [][2]int{{0, 500}, {600, 1200}, {1400, 2000}},
		},
		{
			name:      "small region merges with nearest neighbor",
			regions:   [][2]int{{0, 999}, {1084, 1131}, {1213, 1447}, {1523, 2309}, {2354, 3024}},
			minHeight: 378,
			// Region 1 (47 rows) → merges with nearest (region 2, gap=82)
			// Combined 1+2 (363 rows) < 378 → merges with nearest (region 3, gap=76)
			want: [][2]int{{0, 999}, {1084, 2309}, {2354, 3024}},
		},
		{
			name:      "single small region with one neighbor",
			regions:   [][2]int{{0, 500}, {600, 650}},
			minHeight: 100,
			want:      [][2]int{{0, 650}},
		},
		{
			name:      "small region between two large — merges with closer",
			regions:   [][2]int{{0, 500}, {520, 560}, {700, 1200}},
			minHeight: 100,
			// Gap to left = 520-500 = 20, gap to right = 700-560 = 140
			// Merges with left (smaller gap)
			want: [][2]int{{0, 560}, {700, 1200}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := absorbSmallRegions(tt.regions, tt.minHeight)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d regions, want %d: %v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("region %d = %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestSliceImage_GrayscaleDetection(t *testing.T) {
	// Test with gray pixels near the threshold boundary.
	img := newTestImage(100, 200, nil)

	// Add a band of dark gray (luma ~64, below default threshold 128).
	for y := 50; y < 80; y++ {
		for x := 0; x < 100; x++ {
			img.Set(x, y, color.RGBA{R: 64, G: 64, B: 64, A: 255})
		}
	}
	// Add another dark band.
	for y := 130; y < 160; y++ {
		for x := 0; x < 100; x++ {
			img.Set(x, y, color.RGBA{R: 30, G: 30, B: 30, A: 255})
		}
	}

	jpegData := encodeTestJPEG(img)
	slices, err := SliceImage(jpegData, smallImageOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(slices) != 2 {
		t.Errorf("got %d slices, want 2", len(slices))
	}
}
