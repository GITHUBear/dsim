package denv

type Msg interface {
	String() string
	From() uint64
	To() uint64
}

// delay(ms) : Transmission delay in virtual network
// delayRange(ms) : Actual delay [`delay`, `delay` + `delayRange`)
type EnvOptions struct {
	Delay uint64
	DelayRange uint64
	ClusterSize uint64
	Reliable bool
	MissingRate float64
}

func DefaultEnvOptions() *EnvOptions {
	return  &EnvOptions{
		Delay: 20,
		DelayRange: 10,
		ClusterSize: 3,
		Reliable: true,
	}
}