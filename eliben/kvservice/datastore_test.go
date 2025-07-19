package kvservice

import (
	"testing"
)

func checkPutPrev(t *testing.T, ds *DataStore, k, v, prev string, hasPrev bool) {
	t.Helper()
	prevVal, ok := ds.Put(k, v)
	if hasPrev != ok || prevVal != prev {
		t.Errorf("prevVal=%s, ok=%v; want %s, %v", prevVal, ok, prev, hasPrev)
	}
}

func checkGet(t *testing.T, ds *DataStore, k, v string, found bool) {
	t.Helper()
	val, ok := ds.Get(k)
	if found != ok || val != v {
		t.Errorf("val=%s, ok=%v; want %s, %v", val, ok, v, found)
	}
}

func checkCAS(t *testing.T, ds *DataStore, k, comp, v, prev string, found bool) {
	t.Helper()
	prevVal, ok := ds.CAS(k, comp, v)
	if found != ok || prevVal != prev {
		t.Errorf("prevVal=%s, ok=%v; want %s, %v", prevVal, ok, prev, found)
	}
}

func TestGetPut(t *testing.T) {
	ds := NewDataStore()

	checkGet(t, ds, "foo", "", false)
	checkPutPrev(t, ds, "foo", "bar", "", false)
	checkGet(t, ds, "foo", "bar", true)
	checkPutPrev(t, ds, "foo", "baz", "bar", true)
	checkGet(t, ds, "foo", "baz", true)
	checkPutPrev(t, ds, "nix", "hard", "", false)
}

func TestCASBasic(t *testing.T) {
	ds := NewDataStore()
	ds.Put("foo", "bar")
	ds.Put("sun", "beam")

	checkCAS(t, ds, "foo", "mex", "bro", "bar", true)
	checkCAS(t, ds, "foo", "bar", "bro", "bar", true)
	checkGet(t, ds, "foo", "bro", true)

	checkCAS(t, ds, "goa", "mm", "vv", "", false)
	checkGet(t, ds, "goa", "", false)

	ds.Put("goa", "tva")
	checkCAS(t, ds, "goa", "mm", "vv", "tva", true)
	checkCAS(t, ds, "goa", "mm", "vv", "tva", true)
}

func TestCASConcurrent(t *testing.T) {
	ds := NewDataStore()
	ds.Put("foo", "bar")
	ds.Put("sun", "beam")

	go func() {
		for range 2000 {
			ds.CAS("foo", "bar", "baz")
		}
	}()
	go func() {
		for range 2000 {
			ds.CAS("foo", "baz", "bar")
		}
	}()

	v, _ := ds.Get("foo")
	if v != "bar" && v != "baz" {
		t.Errorf("foo=%s; want bar or baz", v)
	}
}
