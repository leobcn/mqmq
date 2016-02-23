package mqmq

import (
	"bytes"
	"reflect"
	"testing"
)

func TestReadWriteFrame(t *testing.T) {
	tbl := []struct {
		src  frame
		want frame
	}{
		{
			src:  frame{},
			want: frame{},
		},
		{
			src:  frame{nil},
			want: frame{[]byte{}},
		},
		{
			src:  frame{bPut, []byte("test-queue-1"), []byte("Test Message")},
			want: frame{bPut, []byte("test-queue-1"), []byte("Test Message")},
		},
		{
			src:  frame{bPut, []byte("тестовая-очередь-1"), []byte("Тестовое сообщение")},
			want: frame{bPut, []byte("тестовая-очередь-1"), []byte("Тестовое сообщение")},
		},
	}

	buf := &bytes.Buffer{}
	for i, test := range tbl {
		buf.Reset()

		err := writeFrame(buf, test.src, maxFrameLen)
		if err != nil {
			t.Errorf("TestReadWriteFrame #%d: writeFrame failed: %s", i, err)
			continue
		}

		got, err := readFrame(bytes.NewReader(buf.Bytes()), maxFrameLen)
		if err != nil {
			t.Errorf("TestReadWriteFrame #%d: readFrame failed: %s", i, err)
			continue
		}

		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("TestReadWriteFrame #%d: DeepEqual failed: got %#v want %#v", i, got, test.want)
			continue
		}
	}
}

func benchWriteFrame(b *testing.B, f frame) {
	w := &bytes.Buffer{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Reset()
		err := writeFrame(w, f, maxFrameLen)
		if err != nil {
			b.Errorf("writeFrmame failed: %v", err)
		}
	}
}

func BenchmarkWriteFrameS(b *testing.B) {
	benchWriteFrame(b, frame{bOK})
}

func BenchmarkWriteFrameM(b *testing.B) {
	benchWriteFrame(b, frame{bPut, []byte("test-queue-1"), bytes.Repeat([]byte("X"), 100)})
}

func BenchmarkWriteFrameL(b *testing.B) {
	benchWriteFrame(b, frame{bPut, []byte("test-queue-1"), bytes.Repeat([]byte("X"), 10000)})
}

func benchReadFrame(b *testing.B, f frame) {
	buf := &bytes.Buffer{}
	writeFrame(buf, f, maxFrameLen)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := readFrame(bytes.NewReader(buf.Bytes()), maxFrameLen)
		if err != nil {
			b.Errorf("readFrame failed: %v", err)
		}
	}
}

func BenchmarkReadFrameS(b *testing.B) {
	benchReadFrame(b, frame{bOK})
}

func BenchmarkReadFrameM(b *testing.B) {
	benchReadFrame(b, frame{bPut, []byte("test-queue-1"), bytes.Repeat([]byte("X"), 100)})
}

func BenchmarkReadFrameL(b *testing.B) {
	benchReadFrame(b, frame{bPut, []byte("test-queue-1"), bytes.Repeat([]byte("X"), 10000)})
}
