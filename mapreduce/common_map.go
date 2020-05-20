package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string,    // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,     // 实际就是文件的内容
	nReduce int,       // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!

	inputfile, err := os.Open(inFile)
	if err != nil {
		log.Fatal("doMap: open file error", inFile, "error:", err)
	}
	defer inputfile.Close()
	// 打开文件以后我们需要依据内容把其划分为nReduce份

	fileInfo, err := inputfile.Stat() // 我们需要知道文件的大小
	if err != nil {
		log.Fatal("doMap: get state fail", inFile, "error:", err)
	}

	Content := make([]byte, fileInfo.Size()) //接收文件
	ReadBytes, err := inputfile.Read(Content)
	if err != nil {
		log.Fatal("doMap: Read file error", inFile, "error:", err)
	} else if int64(ReadBytes) != fileInfo.Size() {
		log.Fatal("doMap: Read file error, don`t have enough bytes", inFile, "error:", err)
	}

	keyValues := mapF(inFile, string(Content)) // 调用用户编写的Map/Reduce函数 返回一个变长数组

	for i := 0; i < nReduce; i++ {
		filename := reduceName(jobName, mapTaskNumber, i) // 获取存储的文件
		reduceFile, err := os.Create(filename)            // 打开或者创建
		if err != nil {
			log.Fatal("doMap: create intermediate file ", filename, " error: ", err)
		}
		defer reduceFile.Close() // 在一个循环里面可以完成一个文件的写入
		enc := json.NewEncoder(reduceFile) // 使用json格式写入
		for _, kv := range keyValues {
			if ihash(kv.Key)%uint32(nReduce) == uint32(i) { // 查找要存到第N个reduce文件中的键值对
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatal("doMap: encode error:", err)
				}
			}
		}
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
