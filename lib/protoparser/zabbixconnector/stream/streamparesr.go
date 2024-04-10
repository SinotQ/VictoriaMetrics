package stream

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/zabbixconnector"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/writeconcurrencylimiter"
	"github.com/VictoriaMetrics/metrics"
)

var (
	maxLineLen = flagutil.NewBytes("zabbixconnector.maxLineLen", 32*1024*1024, "The maximum length in bytes of a single line accepted by "+
		"/zabbixconnector/api/v1/history")
	addGroups    = flag.Bool("zabbixconnector.addGroups", false, "Enable adding Zabbix host groups to labels (group_name=\"1\").")
	addEmptyTags = flag.Bool("zabbixconnector.addEmptyTags", false, "Enable adding Zabbix tags without values to labels (tag_name=\"1\").")
)

// Parse parses Zabbix Connector http lines from req and calls callback for the parsed rows.
//
// The callback can be called concurrently multiple times for streamed data from req.
//
// callback shouldn't hold rows after returning.
func Parse(r io.Reader, isGzipped bool, callback func(rows []zabbixconnector.Row) error) error {
	wcr := writeconcurrencylimiter.GetReader(r)
	defer writeconcurrencylimiter.PutReader(wcr)
	r = wcr

	if isGzipped {
		zr, err := common.GetGzipReader(r)
		if err != nil {
			return fmt.Errorf("cannot read gzipped zabbixconnector line protocol data: %w", err)
		}
		defer common.PutGzipReader(zr)
		r = zr
	}
	ctx := getStreamContext(r)
	defer putStreamContext(ctx)
	for ctx.Read() {
		uw := getUnmarshalWork()
		uw.ctx = ctx
		uw.callback = callback
		if *addGroups {
			uw.extparams |= zabbixconnector.ADDGROUPS
		}
		if *addEmptyTags {
			uw.extparams |= zabbixconnector.ADDEMPTYTAGS
		}
		uw.reqBuf, ctx.reqBuf = ctx.reqBuf, uw.reqBuf
		ctx.wg.Add(1)
		common.ScheduleUnmarshalWork(uw)
		wcr.DecConcurrency()
	}
	ctx.wg.Wait()
	if err := ctx.Error(); err != nil {
		return err
	}
	return ctx.callbackErr
}

func (ctx *streamContext) Read() bool {
	readCalls.Inc()
	if ctx.err != nil || ctx.hasCallbackError() {
		return false
	}
	ctx.reqBuf, ctx.tailBuf, ctx.err = common.ReadLinesBlockExt(ctx.br, ctx.reqBuf, ctx.tailBuf, maxLineLen.IntN())
	if ctx.err != nil {
		if ctx.err != io.EOF {
			readErrors.Inc()
			ctx.err = fmt.Errorf("cannot read zabbixconnector data: %w", ctx.err)
		}
		return false
	}
	return true
}

var (
	readCalls  = metrics.NewCounter(`vm_protoparser_read_calls_total{type="zabbixconnector"}`)
	readErrors = metrics.NewCounter(`vm_protoparser_read_errors_total{type="zabbixconnector"}`)
	rowsRead   = metrics.NewCounter(`vm_protoparser_rows_read_total{type="zabbixconnector"}`)
)

type streamContext struct {
	br      *bufio.Reader
	reqBuf  []byte
	tailBuf []byte
	err     error

	wg              sync.WaitGroup
	callbackErrLock sync.Mutex
	callbackErr     error
}

func (ctx *streamContext) Error() error {
	if ctx.err == io.EOF {
		return nil
	}
	return ctx.err
}

func (ctx *streamContext) hasCallbackError() bool {
	ctx.callbackErrLock.Lock()
	ok := ctx.callbackErr != nil
	ctx.callbackErrLock.Unlock()
	return ok
}

func (ctx *streamContext) reset() {
	ctx.br.Reset(nil)
	ctx.reqBuf = ctx.reqBuf[:0]
	ctx.tailBuf = ctx.tailBuf[:0]
	ctx.err = nil
	ctx.callbackErr = nil
}

func getStreamContext(r io.Reader) *streamContext {
	select {
	case ctx := <-streamContextPoolCh:
		ctx.br.Reset(r)
		return ctx
	default:
		if v := streamContextPool.Get(); v != nil {
			ctx := v.(*streamContext)
			ctx.br.Reset(r)
			return ctx
		}
		return &streamContext{
			br: bufio.NewReaderSize(r, 64*1024),
		}
	}
}

func putStreamContext(ctx *streamContext) {
	ctx.reset()
	select {
	case streamContextPoolCh <- ctx:
	default:
		streamContextPool.Put(ctx)
	}
}

var streamContextPool sync.Pool
var streamContextPoolCh = make(chan *streamContext, cgroup.AvailableCPUs())

type unmarshalWork struct {
	rows      zabbixconnector.Rows
	ctx       *streamContext
	callback  func(rows []zabbixconnector.Row) error
	extparams byte
	reqBuf    []byte
}

func (uw *unmarshalWork) reset() {
	uw.rows.Reset()
	uw.ctx = nil
	uw.callback = nil
	uw.extparams = 0
	uw.reqBuf = uw.reqBuf[:0]
}

func (uw *unmarshalWork) runCallback(rows []zabbixconnector.Row) {
	ctx := uw.ctx
	if err := uw.callback(rows); err != nil {
		ctx.callbackErrLock.Lock()
		if ctx.callbackErr == nil {
			ctx.callbackErr = fmt.Errorf("error when processing imported data: %w", err)
		}
		ctx.callbackErrLock.Unlock()
	}
	ctx.wg.Done()
}

// Unmarshal implements common.UnmarshalWork
func (uw *unmarshalWork) Unmarshal() {
	uw.rows.Unmarshal(bytesutil.ToUnsafeString(uw.reqBuf), uw.extparams)
	rows := uw.rows.Rows
	rowsRead.Add(len(rows))

	uw.runCallback(rows)
	putUnmarshalWork(uw)
}

func getUnmarshalWork() *unmarshalWork {
	v := unmarshalWorkPool.Get()
	if v == nil {
		return &unmarshalWork{}
	}
	return v.(*unmarshalWork)
}

func putUnmarshalWork(uw *unmarshalWork) {
	uw.reset()
	unmarshalWorkPool.Put(uw)
}

var unmarshalWorkPool sync.Pool
