package raft

func min(first int, second int) int {
	if first > second {
		return second
	}
	return first
}
