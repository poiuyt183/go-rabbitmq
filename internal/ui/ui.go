// Package ui lo phần in ra terminal cho dễ đọc khi test.
// Mỗi tiến trình có 1 màu riêng, ID hiện ở cả 3 log để lần theo 1 event.
package ui

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	reset  = "\033[0m"
	bold   = "\033[1m"
	dim    = "\033[2m"
	red    = "\033[31m"
	green  = "\033[32m"
	yellow = "\033[33m"
	blue   = "\033[34m"
	purple = "\033[35m"
	cyan   = "\033[36m"
	gray   = "\033[90m"
)

// enabled=false khi NO_COLOR được set hoặc output bị pipe vào file.
var enabled = func() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	fi, err := os.Stdout.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}()

func c(code, s string) string {
	if !enabled {
		return s
	}
	return code + s + reset
}

func Dim(s string) string    { return c(gray, s) }
func Bold(s string) string   { return c(bold, s) }
func Green(s string) string  { return c(green, s) }
func Red(s string) string    { return c(red, s) }
func Yellow(s string) string { return c(yellow, s) }
func Cyan(s string) string   { return c(cyan, s) }
func Purple(s string) string { return c(purple, s) }
func Blue(s string) string   { return c(blue, s) }

// outMu giữ cho 1 dòng không bị goroutine khác chen ngang giữa chừng.
// Nhiều "tab trình duyệt" cùng in ra 1 terminal, thiếu khoá là log rối như canh hẹ.
var outMu sync.Mutex

// Logger in ra dạng:  [svc ] 14:23:01 │ nội dung
type Logger struct {
	tag string // đã nhuộm màu sẵn
}

func New(name, color string) *Logger {
	code := map[string]string{
		"cyan": cyan, "green": green, "yellow": yellow,
		"purple": purple, "blue": blue, "red": red,
	}[color]
	return &Logger{tag: c(code+bold, fmt.Sprintf("%-9s", name))}
}

func (l *Logger) Printf(format string, a ...any) {
	outMu.Lock()
	defer outMu.Unlock()
	fmt.Printf("%s %s %s %s\n",
		l.tag,
		Dim(time.Now().Format("15:04:05.000")),
		Dim("│"),
		fmt.Sprintf(format, a...))
}

// Step in dòng con thụt vào, dùng cho từng event trong 1 batch.
// Bắt buộc truyền owner (session id) chứ không để trống: khi nhiều phiên
// chạy song song, dòng con không có chủ sẽ dính nhầm vào header phiên khác.
func (l *Logger) Step(owner, format string, a ...any) {
	outMu.Lock()
	defer outMu.Unlock()
	fmt.Printf("%s %s %s   %s %s\n",
		strings.Repeat(" ", 9),
		strings.Repeat(" ", 12),
		Dim("│"),
		Dim(fmt.Sprintf("%-8s", owner)),
		fmt.Sprintf(format, a...))
}

// Print in nguyên khối nhiều dòng (bảng thống kê) mà không bị chen ngang.
func Print(s string) {
	outMu.Lock()
	defer outMu.Unlock()
	fmt.Println(s)
}

// Box vẽ khung quanh nhiều dòng, dùng cho bảng thống kê định kỳ.
func Box(title string, lines []string) string {
	w := len(title) + 4
	for _, l := range lines {
		if n := visibleLen(l) + 4; n > w {
			w = n
		}
	}
	var b strings.Builder
	b.WriteString(Dim("┌─ ") + Bold(title) + " " + Dim(strings.Repeat("─", max(0, w-len(title)-4))) + "\n")
	for _, l := range lines {
		b.WriteString(Dim("│ ") + l + "\n")
	}
	b.WriteString(Dim("└" + strings.Repeat("─", w-1)))
	return b.String()
}

// visibleLen đếm ký tự thật, bỏ qua escape màu — nếu đếm cả escape thì
// khung sẽ rộng vống lên và lệch hàng.
func visibleLen(s string) int {
	n, inEsc := 0, false
	for _, r := range s {
		switch {
		case r == '\033':
			inEsc = true
		case inEsc && r == 'm':
			inEsc = false
		case !inEsc:
			n++
		}
	}
	return n
}

// Bar vẽ thanh ngang cho funnel: ████████░░░░
func Bar(frac float64, width int) string {
	if frac < 0 {
		frac = 0
	}
	if frac > 1 {
		frac = 1
	}
	full := int(frac*float64(width) + 0.5)
	return Cyan(strings.Repeat("█", full)) + Dim(strings.Repeat("░", width-full))
}
