package util

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
)

/**
 * Description:
 *
 * @author  create by yuxiaoyu
 * @date    in 2020/1/15
 */

func RandomSource(count int) <-chan int {
	out := make(chan int, 1024)

	go func() {
		for i := 0; i < count; i++ {
			out <- rand.Int()
		}
		close(out)
	}()

	return out
}


/**
	read bytes from given source
*/
//func ReadSource(reader io.Reader, chunkSize int) (c chan int) {
//	c = make(chan int, 1024)
//
//	go func() {
//		intBuffer := make([]byte, 8)
//		countNum := 0
//		for {
//			n, err := reader.Read(intBuffer)
//			countNum += n
//			if n > 0 {
//				v := int(binary.BigEndian.Uint64(intBuffer))
//				c <- v
//			}
//			if err != nil || (chunkSize != -1 && countNum >= chunkSize) {
//				break
//			}
//		}
//		close(c)
//	}()
//	return c
//}

func ReadSource(reader io.Reader, chunkSize int) <-chan int {
	out := make(chan int, 1024)

	go func() {
		// int type is 64 bit
		buffer := make([]byte, 8)
		bytesRead := 0

		for {
			n, err := reader.Read(buffer)
			bytesRead += n
			if n > 0 {
				v := int(binary.BigEndian.Uint64(buffer))
				out <- v
			}
			if err != nil || (chunkSize != -1 && bytesRead >= chunkSize) {
				break
			}
		}
		close(out)
	}()
	return out
}

/**
write bytes to given source
*/
func WriterSink(writer io.Writer, in <-chan int) {
	for i := range in {
		var intBuffer = make([]byte, 8)
		binary.BigEndian.PutUint64(intBuffer, uint64(i))
		_, err := writer.Write(intBuffer)
		if err != nil {
			fmt.Println("write file err:" + err.Error())
		}
	}
}
