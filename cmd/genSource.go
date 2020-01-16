package main

import (
	"fmt"
	"github.com/yuqian0218/sort-by-pipeline/util"
	"go-common/app/service/ops/log-agent/pkg/bufio"
	"os"
	"strconv"
)

/**
 * Description:
 *
 * @author  create by yuxiaoyu
 * @date    in 2020/1/15
 */
const fileInName = "small.in"
const fileOutName = "small.out"
const numCount = 80000000
const chuckCount = 4

func main() {
	//prepareData()
	//sortPipeline := createSortPipeline()
	sortPipeline := createNetworkPipeline()
	writeToFile(sortPipeline)
	printFile()
}

func prepareData() {
	sourceCh := util.RandomSource(numCount)
	file, err := os.Create(fileInName)
	if err != nil {
		panic("create file err:" + err.Error())
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	util.WriterSink(file, sourceCh)
}

func createSortPipeline() (out <-chan int) {
	util.Init()
	out = make(chan int, 1024)
	//read by trucks
	chunkSize := numCount * 8 / chuckCount
	chs := make([]<-chan int,chuckCount)
	for i := 0; i < chuckCount; i++ {
		file, err := os.Open(fileInName)
		if err != nil {
			fmt.Println("read file err:" + err.Error())
		}
		_, _ = file.Seek(int64(i*chunkSize), 0)
		ch := util.SortInMem(util.ReadSource(bufio.NewReader(file), chunkSize))
		chs[i] = ch
	}
	out = util.MergeN(chs)
	return
}

func createNetworkPipeline() <-chan int {
	util.Init()
	sortAddr := []string{}

	//read by trucks
	chunkSize := numCount * 8 / chuckCount
	for i := 0; i < chuckCount; i++ {
		file, err := os.Open(fileInName)
		if err != nil {
			fmt.Println("read file err:" + err.Error())
		}

		source := util.ReadSource(bufio.NewReader(file), chunkSize)

		addr := ":" + strconv.Itoa(7000+i)
		util.NetworkSink(addr, util.SortInMem(source))

		sortAddr = append(sortAddr, addr)
	}

	var sortResults []<-chan int
	for _, addr := range sortAddr {
		sortResults = append(sortResults, util.NetworkReader(addr))
	}

	return util.MergeN(sortResults)
}

//func createPipeline(fileName string, fileSize int, chunkCount int) <-chan int {
//	chunkSize := fileSize*8 / chunkCount
//
//	var sortResults []<-chan int
//
//	for i := 0; i < chunkCount; i++ {
//		file, err := os.Open(fileName)
//		if err != nil {
//			panic(err)
//		}
//
//		file.Seek(int64(i*chunkSize), 0)
//
//		source := util.ReadSource(bufio.NewReader(file), chunkSize)
//
//		sortResults = append(sortResults, util.SortInMem(source))
//	}
//
//	return util.MergeN(sortResults...)
//}

func printFile() {
	file, err := os.Open(fileOutName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	//only print 100 in header
	ch := util.ReadSource(file, 100)
	for i := range ch {
		fmt.Printf("sorted print: %d\n", i)
	}
}

//func printFile(filename string) {
//	file, err := os.Open(filename)
//	if err != nil {
//		panic(err)
//	}
//	defer file.Close()
//
//	p := util.ReadSource(file, -1)
//	count := 0
//	for v := range p {
//		fmt.Println(v)
//		count++
//		if count >= 100 {
//			break
//		}
//	}
//}

func writeToFile(ch <-chan int) {
	fmt.Println("begin to write file")
	file, err := os.Create(fileOutName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()
	util.WriterSink(writer, ch)
}

//func writeToFile(p <-chan int, filename string) {
//	file, err := os.Create(filename)
//	if err != nil {
//		panic(err)
//	}
//	defer file.Close()
//
//	writer := bufio.NewWriter(file)
//	defer writer.Flush()
//
//	util.WriterSink(writer, p)
//}
