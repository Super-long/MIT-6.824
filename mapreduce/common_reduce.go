package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string,       // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int,             // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	KeyValues := make(map[string][]string, 0)
	// 其实这里得到的是所有map的一个分区的key
	// 每一次执行这个函数的作用就是把所有map的一个分区合并到一起
	// 这样reduce执行完毕以后我们就有了nmap个合并后的文件分布在不同的reduce worker上
	// 最后只需要执行一次合并就可以了
	for i := 0; i < nMap; i++ {
		filename := reduceName(jobName, i, reduceTaskNumber) //获取当前reduce的输入文件 也就是map中写入的文件
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("doReduce: open file error ", filename, "error", err)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break //解析完毕
			}

			_, ok := KeyValues[kv.Key] //在map中是靠哈希分开的,所以同一个文件中的key是不一样的,且相同的key可能存在不同的value
			if !ok {                   //不存在的话
				KeyValues[kv.Key] = make([]string, 0)
			}
			KeyValues[kv.Key] = append(KeyValues[kv.Key], kv.Value)
		}
	}

	var keys []string
	for k, _ := range KeyValues {
		keys = append(keys, k)
	}
	sort.Strings(keys) // 给所有的key排序 如果go的map天然有序就不用排了

	mergeFileName := mergeName(jobName, reduceTaskNumber) // 根据提示 我们可以得到最后合并的文件的名称
	mergeFile, err := os.Create(mergeFileName)
	if err != nil {
		log.Fatal("doReduce: create merge file error ", mergeFileName, " error: ", err)
	}
	defer mergeFile.Close()

	enc := json.NewEncoder(mergeFile) //以json格式写入最终文件

	for _, k := range keys { // 顺序处理所有的key
		res := reduceF(k, KeyValues[k])
		err := enc.Encode(&KeyValue{k, res})
		if err != nil {
			log.Fatal("doReduce: encode error")
		}
	}
}
