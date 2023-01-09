package utils

import (
	"bytes"
	"github.com/hardcore-os/corekv/utils/codec"
	"math/rand"
	"sync"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	header := &Element{
		levels: make([]*Element, defaultMaxLevel),
	}
	return &SkipList{
		header:   header,
		maxLevel: defaultMaxLevel - 1,
		rand:     r,
	}
}

type Element struct {
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level+1),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	list.lock.Lock()
	defer list.lock.Unlock()

	prevs := make([]*Element, list.maxLevel+1)
	// 计算分数
	key := data.Key
	score := list.calcScore(key)

	header, maxLevel := list.header, list.maxLevel
	prev := header
	for i := maxLevel; i >= 0; i-- {
		prevs[i] = prev
		for ne := prev.levels[i]; ne != nil; ne = prev.levels[i] {
			if comp:=list.compare(score, key,ne);comp <=0 {
				if comp == 0 {
					ne.entry.Value = data.Value
					return nil
				}
				break
			}
			prev = ne
			prevs[i] = prev
		}
	}

	level := list.randLevel()
	elem := newElement(score,data,level)
	for i := level;i >= 0;i-- {
		ne := prevs[i].levels[i]
		prevs[i].levels[i] = elem
		elem.levels[i] = ne
	}
	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	list.lock.RLock()
	defer list.lock.RUnlock()

	// 1.计算key的分数
	score := list.calcScore(key)
	// 得到起始节点和起始层数
	header, maxLevel := list.header, list.maxLevel
	prev := header
	for i := maxLevel; i >= 0; i-- {
		for ne := prev.levels[i]; ne != nil; ne = prev.levels[i] {
			if comp := list.compare(score, key, ne); comp <= 0 {
				if comp == 0 {
					return ne.Entry()
				}
				break
			}
			prev = ne
		}
	}
	return
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	// 1. 首先比较分数 如果分数相同则直接比较key字节数组的大小
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}
	// 分数不同直接返回分数的结果
	if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) randLevel() int {
	// 1. 遍历跳表的所有层，通过随机数判断是否返回当前层
	for i := 0; i < list.maxLevel; i++ {
		if list.rand.Intn(2) == 0 {
			return i
		}
	}
	// 否则直接返回最大层
	return list.maxLevel
}

func (list *SkipList) Size() int64 {
	return list.size
}
