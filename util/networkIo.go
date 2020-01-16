package util

import (
	"go-common/app/service/ops/log-agent/pkg/bufio"
	"net"
)

/**
 * Description:
 *
 * @author  create by yuxiaoyu
 * @date    in 2020/1/16
 */
func NetworkReader(addr string) <-chan int {
	out := make(chan int)
	go func() {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			panic(err)
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		ch := ReadSource(reader, -1)

		for v := range ch {
			out <- v
		}
		close(out)
	}()

	return out
}

func NetworkSink(addr string, ch <-chan int) {
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}

	go func() {
		defer listen.Close()
		accept, err := listen.Accept()
		if err != nil {
			panic(err)
		}
		defer accept.Close()

		writer := bufio.NewWriter(accept)
		defer writer.Flush()

		WriterSink(writer, ch)
	}()
}

