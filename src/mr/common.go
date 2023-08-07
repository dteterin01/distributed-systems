package mr

import "strconv"

func reduceName(workerId int, mapTask int, nReduce int) string {
	return "mrtmp." + strconv.Itoa(workerId) + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(nReduce)
}
