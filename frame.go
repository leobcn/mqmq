package mqmq

import (
	"encoding/binary"
	"errors"
	"io"
)

type frame [][]byte

// ErrFrameLen means that the maximum frame length is exceeded.
var ErrFrameLen = errors.New("mqmq: frame too large")

//ErrFrameFormat means that the frame is corrupted and cannot be read.
var ErrFrameFormat = errors.New("mqmq: bad frame format")

func readFrame(r io.Reader, maxFrameLen uint32) (frame, error) {
	var err error
	var buf4 [4]byte

	_, err = io.ReadFull(r, buf4[:])
	if err != nil {
		return nil, err
	}
	frameLen := binary.BigEndian.Uint32(buf4[:])

	if frameLen > maxFrameLen {
		return nil, ErrFrameLen
	}

	buf := make([]byte, frameLen, frameLen)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	f := make(frame, 0, 3)

	var i uint32
	for i < frameLen {
		if i+4 > frameLen {
			return nil, ErrFrameFormat
		}
		itemLen := binary.BigEndian.Uint32(buf[i : i+4])
		i += 4

		if i+itemLen > frameLen {
			return nil, ErrFrameFormat
		}
		f = append(f, buf[i:i+itemLen])
		i += itemLen
	}

	return f, nil
}

func writeFrame(w io.Writer, f frame, maxFrameLen uint32) error {
	var err error
	var buf4 [4]byte

	var frameLen uint32
	for _, item := range f {
		frameLen += 4 + uint32(len(item))
	}

	if frameLen > maxFrameLen {
		return ErrFrameLen
	}

	binary.BigEndian.PutUint32(buf4[:], frameLen)
	_, err = w.Write(buf4[:])
	if err != nil {
		return err
	}

	for _, item := range f {
		binary.BigEndian.PutUint32(buf4[:], uint32(len(item)))
		_, err = w.Write(buf4[:])
		if err != nil {
			return err
		}

		_, err = w.Write(item)
		if err != nil {
			return err
		}
	}

	return nil
}
