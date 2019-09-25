// Package logfilerotater provides log file rotating functionality in Go with io.Writer interface which make it very easy to integrate with other Go packages.
// logfilerotater 是一个具有日志文件切割功能的 io.Writer 接口实现，非常容易与其他Go模块集成。
package logfilerotater

import (
	"log"
	"os"
	"path"
	"storage/image/util"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// RotateStrategy 日志切割策略
// 建议策略为 hourfile/dayfile ，因为 hour/day 策略 rsyslog 收集日志有时候会感知不到新文件
type RotateStrategy string

const (
	// "hour" : 总是往基准路径写日志，遇到整点将基准文件重命名为带上一小时标记的文件名，再创建基准路径文件写入日志
	RotateStrategyHour RotateStrategy = "hour"
	// "day" : 总是往基准路径写日志，遇到零点将基准文件重命名为带上一天标记的文件名，再创建基准路径文件写入日志
	RotateStrategyDay RotateStrategy = "day"
	// "hourfile" : 总是往"基准路径.YYYYMMDDHH"写日志内容，遇到整点创建新的日志文件. 此为默认策略
	RotateStrategyHourFile RotateStrategy = "hourfile"
	// "dayfile" : 总是往"基准路径.YYYYMMDD"写日志内容，遇到零点创建新的日志文件
	RotateStrategyDayFile RotateStrategy = "dayfile"
	// "none" : 总是往基准路径写日志，不切割
	RotateStrategyNone RotateStrategy = "none"
)

// LogFileRotater 日志文件Writer
type LogFileRotater struct {
	logPath            string // 日志文件基准路径
	rotate             RotateStrategy
	createdOrRotatedAt time.Time // 创建时刻或最近一次切割时刻
	nextRotateAt       time.Time // 下次切割时刻
	fp                 *os.File
	lock               *sync.RWMutex
}

// New 创建一个 LogFileRotater
func New(logPath string, rotate RotateStrategy) *LogFileRotater {
	if !(rotate == RotateStrategyHour ||
		rotate == RotateStrategyDay ||
		rotate == RotateStrategyHourFile ||
		rotate == RotateStrategyDayFile ||
		rotate == RotateStrategyNone) {
		log.Println("[WARN] rotate:", rotate, "is not a valid RotateStrategy. \"hourfile\" will be used.")
		rotate = RotateStrategyHourFile
	}

	rotater := &LogFileRotater{
		logPath:            logPath,
		rotate:             rotate,
		createdOrRotatedAt: time.Time{},
		nextRotateAt:       time.Time{},
		fp:                 nil,
		lock:               new(sync.RWMutex),
	}

	rotater.createdOrRotatedAt = time.Now()
	rotater.nextRotateAt = getNextRotateTime(rotate, rotater.createdOrRotatedAt)

	realLogPath := getRealLogPath(logPath, rotate, rotater.createdOrRotatedAt)

	if !util.FileExists(realLogPath) {
		logDir := path.Dir(realLogPath)
		if e := os.MkdirAll(logDir, os.ModeDir|0755); e != nil {
			log.Println("[ERROR] Log path mkdir fail:", e, ". Stderr will be used. log dir:", logDir)
			rotater.fp = os.Stderr
			rotater.rotate = RotateStrategyNone
			rotater.nextRotateAt = getNextRotateTime(RotateStrategyNone, rotater.createdOrRotatedAt)
			return rotater
		}
	}

	if fp, e := os.OpenFile(realLogPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666); e != nil {
		log.Println("[ERROR] Log file create fail:", e, ". Stderr will be used. log file:", realLogPath)
		rotater.fp = os.Stderr
		rotater.rotate = RotateStrategyNone
		rotater.nextRotateAt = getNextRotateTime(RotateStrategyNone, rotater.createdOrRotatedAt)
	} else {
		rotater.fp = fp
	}

	return rotater
}

// NewStderr 创建一个输出到 os.Stderr 的 LogFileRotater
func NewStderr() *LogFileRotater {
	return &LogFileRotater{
		logPath:            "/dev/null",
		rotate:             RotateStrategyNone,
		createdOrRotatedAt: time.Time{},
		nextRotateAt:       time.Time{},
		fp:                 os.Stderr,
		lock:               nil,
	}
}

// Write 实现 io.Writer 接口，带日志切割功能
func (r *LogFileRotater) Write(b []byte) (int, error) {
	// 若不切割日志，则直接写入，无需加锁
	if r.rotate == RotateStrategyNone {
		return r.fp.Write(b)
	}

	// 写入时刻，后续所有时间操作都以此为基准，避免时间调整、闰秒等因素影响出现细微的时间相关bug
	var now time.Time

	r.lock.RLock()
	now = time.Now()
	if now.Before(r.nextRotateAt) {
		defer r.lock.RUnlock()
		return r.fp.Write(b)
	}
	r.lock.RUnlock()

	r.lock.Lock()
	defer r.lock.Unlock()

	// 关闭旧文件
	if r.fp.Fd() > uintptr(syscall.Stderr) {
		r.fp.Close()
	}

	// 旧文件 rename
	if r.rotate == RotateStrategyHour || r.rotate == RotateStrategyDay {
		var backupLogPath string
		if r.rotate == RotateStrategyHour {
			backupLogPath = getRealLogPath(r.logPath, RotateStrategyHourFile, r.createdOrRotatedAt)
		} else {
			backupLogPath = getRealLogPath(r.logPath, RotateStrategyDayFile, r.createdOrRotatedAt)
		}

		if util.FileExists(backupLogPath) {
			backupLogPath = backupLogPath + "." + strconv.FormatInt(time.Now().UnixNano(), 10)
		}
		if e := os.Rename(r.logPath, backupLogPath); e != nil {
			log.Printf("[ERROR] log file rename fail. Stderr will be used as log writer. err: %v. oldpath: %v newpath: %v\n", e, r.logPath, backupLogPath)
			r.fp = os.Stderr
			r.rotate = RotateStrategyNone

			r.createdOrRotatedAt = now
			r.nextRotateAt = getNextRotateTime(r.rotate, now)

			return r.fp.Write(b)
		}
	}

	// 打开新文件
	realLogPath := getRealLogPath(r.logPath, r.rotate, now)
	if fp, e := os.OpenFile(realLogPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666); e != nil {
		log.Println("[ERROR] Log file create fail:", e, ". Stderr will be used. log file:", realLogPath)
		r.fp = os.Stderr
		r.rotate = RotateStrategyNone
	} else {
		r.fp = fp
	}

	// 写入
	r.createdOrRotatedAt = now
	r.nextRotateAt = getNextRotateTime(r.rotate, now)
	return r.fp.Write(b)
}

// getNextRotateTime 计算下次切割时刻
func getNextRotateTime(rotate RotateStrategy, now time.Time) time.Time {
	if rotate == RotateStrategyHour || rotate == RotateStrategyHourFile {
		future := now.Add(time.Hour)
		return time.Date(future.Year(), future.Month(), future.Day(), future.Hour(), 0, 0, 0, future.Location())
	} else if rotate == RotateStrategyDay || rotate == RotateStrategyDayFile {
		future := now.Add(time.Hour * 24)
		return time.Date(future.Year(), future.Month(), future.Day(), 0, 0, 0, 0, future.Location())
	} else {
		return time.Date(3000, 1, 1, 0, 0, 0, 0, now.Location())
	}
}

// getRealLogPath 获取日志文件真正的写入路径
func getRealLogPath(logPath string, rotate RotateStrategy, tm time.Time) string {
	if rotate == RotateStrategyHourFile {
		return logPath + "." + tm.Format("2006010215")
	} else if rotate == RotateStrategyDayFile {
		return logPath + "." + tm.Format("20060102")
	}
	return logPath
}
