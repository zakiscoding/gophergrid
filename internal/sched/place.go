// Package sched is the leader's scheduling logic: placement, the assignment loop, lease
// reaping and dead-worker detection.
package sched

import (
	"sort"

	"github.com/zakiscoding/gophergrid/internal/jobs"
	"github.com/zakiscoding/gophergrid/internal/store"
)

// Node is the leader's view of one worker during a scheduling pass.
type Node struct {
	ID       string
	Capacity jobs.Resources
	GPUs     []store.GPUInfo
	Types    map[string]bool
	Used     jobs.Resources
	UsedGPUs map[string]bool
}

// NewNode builds a Node from a registry row and its current usage.
func NewNode(w store.Worker, u store.Usage) *Node {
	n := &Node{ID: w.ID, Capacity: jobs.Resources{CPUMillis: w.CPUMillis, MemoryMB: w.MemoryMB, GPUs: len(w.GPUs)},
		GPUs: w.GPUs, Types: map[string]bool{}, Used: u.Resources, UsedGPUs: map[string]bool{}}
	for _, t := range w.JobTypes {
		n.Types[t] = true
	}
	for _, g := range u.GPUIDs {
		n.UsedGPUs[g] = true
	}
	return n
}

// load is the node's utilization after adding r: the larger of its CPU and memory fractions.
func (n *Node) load(r jobs.Resources) float64 {
	cpu := float64(n.Used.CPUMillis+r.CPUMillis) / float64(max(n.Capacity.CPUMillis, 1))
	mem := float64(n.Used.MemoryMB+r.MemoryMB) / float64(max(n.Capacity.MemoryMB, 1))
	return max(cpu, mem)
}

// freeGPUs returns unused GPUs with at least memMB memory, smallest first.
func (n *Node) freeGPUs(memMB int64) []store.GPUInfo {
	var out []store.GPUInfo
	for _, g := range n.GPUs {
		if !n.UsedGPUs[g.ID] && g.MemoryMB >= memMB {
			out = append(out, g)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].MemoryMB < out[j].MemoryMB })
	return out
}

// fits reports whether j can run on n now and, for GPU jobs, how many free GPUs (of any
// size) the node would have left. It runs for every (job, node) pair, so it doesn't
// allocate; pickGPUs chooses the actual GPUs once a node has won.
func (n *Node) fits(j jobs.Job) (gpusLeft int, ok bool) {
	r := j.Resources
	if !n.Types[j.Type] ||
		n.Used.CPUMillis+r.CPUMillis > n.Capacity.CPUMillis ||
		n.Used.MemoryMB+r.MemoryMB > n.Capacity.MemoryMB {
		return 0, false
	}
	if r.GPUs == 0 {
		return 0, true
	}
	free, big := 0, 0
	for _, g := range n.GPUs {
		if !n.UsedGPUs[g.ID] {
			free++
			if g.MemoryMB >= r.GPUMemoryMB {
				big++
			}
		}
	}
	return free - r.GPUs, big >= r.GPUs
}

// pickGPUs returns the smallest free GPUs that are big enough (best fit).
func (n *Node) pickGPUs(j jobs.Job) []string {
	if j.Resources.GPUs == 0 {
		return nil
	}
	free := n.freeGPUs(j.Resources.GPUMemoryMB)
	ids := make([]string, j.Resources.GPUs)
	for i := range ids {
		ids[i] = free[i].ID
	}
	return ids
}

func (n *Node) take(j jobs.Job, gpus []string) {
	n.Used.CPUMillis += j.Resources.CPUMillis
	n.Used.MemoryMB += j.Resources.MemoryMB
	n.Used.GPUs += len(gpus)
	for _, g := range gpus {
		n.UsedGPUs[g] = true
	}
}

// Policy tunes placement.
type Policy struct {
	// KeepGPUNodesFree steers CPU-only jobs away from GPU workers while a CPU-only worker
	// has room, so GPU jobs don't queue behind CPU work on the only GPU node.
	KeepGPUNodesFree bool
}

// Place assigns jobs, in the order given (the queue order), to nodes. It is greedy:
//   - GPU jobs go to the node where they fit with the least leftover GPU capacity (bin
//     packing keeps whole GPUs and whole GPU nodes free for big jobs).
//   - CPU jobs go to the least-loaded node that fits (spreading load), preferring nodes
//     without GPUs when KeepGPUNodesFree is set.
//
// A job that fits nowhere is skipped, and smaller jobs behind it may still run (backfill).
func Place(queued []jobs.Job, nodes []*Node, p Policy) []store.Assignment {
	var out []store.Assignment
	for _, j := range queued {
		var best *Node
		bestScore := 0.0
		for _, n := range nodes {
			gpusLeft, ok := n.fits(j)
			if !ok {
				continue
			}
			var score float64
			if j.Resources.GPUs > 0 {
				// Fewer free GPUs left afterwards is better; break ties on load.
				score = float64(gpusLeft)*10 + n.load(j.Resources)
			} else {
				score = n.load(j.Resources)
				if p.KeepGPUNodesFree && len(n.GPUs) > 0 {
					score += 10 // only if no CPU-only node fits
				}
			}
			if best == nil || score < bestScore || (score == bestScore && n.ID < best.ID) {
				best, bestScore = n, score
			}
		}
		if best == nil {
			continue
		}
		gpus := best.pickGPUs(j)
		best.take(j, gpus)
		out = append(out, store.Assignment{JobID: j.ID, WorkerID: best.ID, GPUIDs: gpus})
	}
	return out
}
