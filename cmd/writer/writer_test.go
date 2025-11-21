package main

import "testing"

func TestIntToAlphabetKey(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected string
	}{
		// Basic single letter cases
		{"zero", 0, "a"},
		{"one", 1, "b"},
		{"two", 2, "c"},
		{"twenty_five", 25, "z"},

		// Two letter cases
		{"twenty_six", 26, "aa"},
		{"twenty_seven", 27, "ab"},
		{"twenty_eight", 28, "ac"},
		{"fifty_one", 51, "az"},
		{"fifty_two", 52, "ba"},
		{"fifty_three", 53, "bb"},
		{"seventy_seven", 77, "bz"},
		{"seventy_eight", 78, "ca"},

		// Three letter cases
		{"six_hundred_seventy_six", 676, "za"},
		{"six_hundred_seventy_seven", 677, "zb"},
		{"seven_hundred_two", 702, "aaa"},
		{"seven_hundred_three", 703, "aab"},

		// Edge cases
		{"negative", -1, ""},
		{"negative_large", -100, ""},

		// Larger numbers
		{"thousand", 1000, "alm"},
		{"ten_thousand", 10000, "ntq"},
		{"hundred_thousand", 100000, "eqxe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := intToAlphabetKey(tt.input)
			if result != tt.expected {
				t.Errorf("intToAlphabetKey(%d) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIntToAlphabetKeySequential(t *testing.T) {
	// Test sequential values to ensure consistency
	expected := []string{
		"a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
		"k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
		"u", "v", "w", "x", "y", "z", "aa", "ab", "ac", "ad",
	}

	for i := int64(0); i < int64(len(expected)); i++ {
		result := intToAlphabetKey(i)
		if result != expected[i] {
			t.Errorf("intToAlphabetKey(%d) = %q, want %q", i, result, expected[i])
		}
	}
}

func TestIntToAlphabetKeyBijective(t *testing.T) {
	// Test that the function produces unique outputs for different inputs
	seen := make(map[string]int64)
	for i := int64(0); i < 1000; i++ {
		key := intToAlphabetKey(i)
		if key == "" {
			t.Errorf("intToAlphabetKey(%d) returned empty string", i)
			continue
		}
		if prev, exists := seen[key]; exists {
			t.Errorf("intToAlphabetKey produced duplicate key %q for inputs %d and %d", key, prev, i)
		}
		seen[key] = i
	}
}
