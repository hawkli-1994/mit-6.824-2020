package mr


const (
	MapTaskType int = 0
	ReduceTaskType int = 1
)

const (
	mIdle     int = 0
	mMaping   int = 1
	mMapend   int = 4
	mReducing int = 2
	mDone     int = 3
)

type TaskInfo struct {
	ID       int
	status   int
	alive    bool
	done     bool
	Filename string
	nReduce  int
	Index    int
	Typo     int
}

type MasterInfo struct {
	Status int
	NReduce int
}

const (
	tNull int = -1
	tPending int = 0
	tRunning int = 1
	tSuccess int = 2
	tFailure int = 3
	tTimeout int = 4
)
