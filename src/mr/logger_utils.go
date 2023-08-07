package mr

import "log"

func checkError(err error, errorMessage string) bool {
	if err != nil {
		logError(err, errorMessage)
		return true
	}
	return false
}

func logError(err error, errorMessage string) {
	log.Fatalf("%s, %s", err, errorMessage)
}
