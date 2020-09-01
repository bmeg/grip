package gripper

/*
The Channel Multiplexer is designed to allow the user to create multiple processing
pipelines, each with a different input/output channel. The multiplexer then takes
set of inputs, each labeled by the processing channel number. It then aggregates
the outputs of all of the channels and returns them in the same order that they
were added in.

So

o := m.GetOutChannel()

c1, err := m.AddPipeline(in1, out1)
c2, err := m.AddPipeline(in2, out2)

m.Put(c1, val1)
m.Put(c2, val2)
m.Put(c1, val3)
m.Put(c2, val4)

out1 <- o // result val1 from pipeline c1
out2 <- o // result val2 from pipeline c2
out3 <- o // result val3 from pipeline c1
out4 <- o // result val4 from pipeline c2

The order is preserved, regardless of runtime of pipeline between in1 -> out1
and in2 -> out2
*/

type ChannelMux struct {
	messageOrder chan int
	inputs       []chan<- interface{}
	outputs      []<-chan interface{}
	outChannel   chan interface{}
}

func runMux(m *ChannelMux) {
	for n := range m.messageOrder {
		t := <-m.outputs[n]
		m.outChannel <- t
	}
	close(m.outChannel)
}

func NewChannelMux() *ChannelMux {
	out := ChannelMux{
		messageOrder: make(chan int, 10),
		inputs:       make([]chan<- interface{}, 0, 10),
		outputs:      make([]<-chan interface{}, 0, 10),
		outChannel:   make(chan interface{}, 10),
	}
	go runMux(&out)
	return &out
}

func (m *ChannelMux) Close() {
	for _, c := range m.inputs {
		close(c)
	}
	close(m.messageOrder)
}

func (m *ChannelMux) AddPipeline(input chan<- interface{}, output <-chan interface{}) (int, error) {
	i := len(m.inputs)
	m.inputs = append(m.inputs, input)
	m.outputs = append(m.outputs, output)
	return i, nil
}

func (m *ChannelMux) Put(num int, d interface{}) {
	m.inputs[num] <- d
	m.messageOrder <- num
}

func (m *ChannelMux) GetOutChannel() <-chan interface{} {
	return m.outChannel
}
