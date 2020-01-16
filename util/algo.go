package util

import (
	"fmt"
	"sort"
	"time"
)

var startTime time.Time

func Init(){
	startTime = time.Now()
}
/**
 * Description:
 *
 * @author  create by yuxiaoyu
 * @date    in 2020/1/15
 */
func SortInMem(in <-chan int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		var intArray = make([]int, 0)
		for i := range in {
			intArray = append(intArray, i)
		}
		sort.Ints(intArray)

		// Output
		for _, v := range intArray {
			out <- v
		}
		close(out)
	}()
	fmt.Println("Sort in mem finished in:",time.Now().Sub(startTime))
	return out
}

//func SortInMem(in <-chan int) <-chan int {
//	out := make(chan int, 1024)
//
//	go func() {
//		// Read into memory
//		a := []int{}
//		for v := range in {
//			a = append(a, v)
//		}
//
//		// Sort
//		sort.Ints(a)
//
//		// Output
//		for _, v := range a {
//			out <- v
//		}
//		close(out)
//	}()
//
//	return out
//}

func MergeN(chs []<-chan int) (out <-chan int) {
	out = make(chan int, 1024)

	chNum := len(chs)
	if chNum == 1 {
		out = chs[0]
	} else {
		i := chNum / 2
		ch1 := MergeN(chs[:i])
		ch2 := MergeN(chs[i:])
		out = merge2(ch1, ch2)
	}
	fmt.Println("mergeN finished in:",time.Now().Sub(startTime))
	return
}

//func MergeN(inputs ...<-chan int) <-chan int {
//	if len(inputs) == 1 {
//		return inputs[0]
//	}
//
//	m := len(inputs) / 2
//	// merge inputs [0..m) and inputs [m..end)
//	return merge2(MergeN(inputs[:m]...), MergeN(inputs[m:]...))
//}

func merge2(ch1, ch2 <-chan int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		i1, ok1 := <-ch1
		i2, ok2 := <-ch2
		for ok1 || ok2 {
			if !ok1 || (ok2 && i1 > i2) {
				out <- i2
				i2, ok2 = <-ch2
			} else {
				out <- i1
				i1, ok1 = <-ch1
			}
		}
		close(out)
	}()
	return out
}

//func merge2(in1, in2 <-chan int) <-chan int {
//	out := make(chan int, 1024)
//
//	go func() {
//		v1, ok1 := <-in1
//		v2, ok2 := <-in2
//		for ok1 || ok2 {
//			if !ok2 || (ok1 && v1 <= v2) {
//				out <- v1
//				v1, ok1 = <-in1
//			} else {
//				out <- v2
//				v2, ok2 = <-in2
//			}
//		}
//		close(out)
//		//fmt.Println("Merge done:", time.Now().Sub(startTime))
//	}()
//
//	return out
//}
