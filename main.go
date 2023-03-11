package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
    numProcesses := len(processes)
    completedProcesses := make([]Process, 0, numProcesses)

    currentTime := 0
    for len(completedProcesses) < numProcesses {
        // Find the next process with the highest priority and shortest burst time
        nextProcessIndex := -1
        for i, p := range processes {
            if p.ArrivalTime <= currentTime && !p.Completed {
                if nextProcessIndex == -1 || p.Priority < processes[nextProcessIndex].Priority || (p.Priority == processes[nextProcessIndex].Priority && p.BurstTime < processes[nextProcessIndex].BurstTime) {
                    nextProcessIndex = i
                }
            }
        }

        // If no processes are ready to run, move to the next arrival time
        if nextProcessIndex == -1 {
            minArrivalTime := math.MaxInt32
            for _, p := range processes {
                if !p.Completed && p.ArrivalTime < minArrivalTime {
                    minArrivalTime = p.ArrivalTime
                }
            }
            currentTime = minArrivalTime
            continue
        }

        // Run the selected process
        selectedProcess := &processes[nextProcessIndex]
        selectedProcess.StartTime = currentTime
        selectedProcess.Completed = true
        completedProcesses = append(completedProcesses, *selectedProcess)
        currentTime += selectedProcess.BurstTime
        selectedProcess.TurnaroundTime = currentTime - selectedProcess.ArrivalTime
        selectedProcess.WaitingTime = selectedProcess.TurnaroundTime - selectedProcess.BurstTime

        // Print the selected process details
        fmt.Fprintf(w, "%s %d: %s\n", title, currentTime, selectedProcess)
    }

    // Print the average waiting time and turnaround time for all processes
    avgWaitingTime, avgTurnaroundTime := calculateAverageTimes(completedProcesses)
    fmt.Fprintf(w, "Average waiting time: %.2f\n", avgWaitingTime)
    fmt.Fprintf(w, "Average turnaround time: %.2f\n", avgTurnaroundTime)
}
func SJFSchedule(w io.Writer, title string, processes []Process) {
    // Sort processes by their burst time (i.e. duration).
    sort.Slice(processes, func(i, j int) bool {
        return processes[i].BurstTime < processes[j].BurstTime
    })

    // Run the processes.
    currentTime := 0
    for _, p := range processes {
        fmt.Fprintf(w, "Process %s runs from time %d to %d\n", p.Name, currentTime, currentTime+p.BurstTime)
        currentTime += p.BurstTime
    }

    // Print the average waiting time.
    avgWaitTime := calculateAverageWaitingTime(processes)
    fmt.Fprintf(w, "%s Average waiting time: %.2f\n", title, avgWaitTime)
}

// calculateAverageWaitingTime calculates the average waiting time for a list of processes.
func calculateAverageWaitingTime(processes []Process) float64 {
    totalWaitTime := 0
    for i, p := range processes {
        waitTime := 0
        for j := 0; j < i; j++ {
            waitTime += processes[j].BurstTime
        }
        totalWaitTime += waitTime
    }
    return float64(totalWaitTime) / float64(len(processes))
}
func RRSchedule(w io.Writer, title string, processes []Process) {
    // Quantum time
    const quantum = 2

    // Keep track of the remaining burst time for each process
    remaining := make([]int, len(processes))
    for i := range processes {
        remaining[i] = processes[i].Burst
    }

    // Keep track of the time elapsed
    var elapsed int

    // Keep track of the processes in the ready queue
    var ready []int

    // Keep looping until all processes have completed
    for len(ready) > 0 || len(processes) > 0 {
        // Add any new processes to the ready queue
        for i, p := range processes {
            if p.Arrival <= elapsed {
                ready = append(ready, i)
                processes = append(processes[:i], processes[i+1:]...)
                remaining = append(remaining[:i], remaining[i+1:]...)
                i--
            }
        }

        // If there are no processes in the ready queue, we need to wait
        if len(ready) == 0 {
            fmt.Fprintf(w, "%d Idle\n", elapsed)
            elapsed++
            continue
        }

        // Get the index of the next process to run
        index := ready[0]
        ready = ready[1:]

        // Run the process for the quantum time, or until it finishes
        if remaining[index] > quantum {
            fmt.Fprintf(w, "%d %s\n", elapsed, processes[index].Name)
            remaining[index] -= quantum
            elapsed += quantum
            ready = append(ready, index)
        } else {
            fmt.Fprintf(w, "%d %s\n", elapsed, processes[index].Name)
            elapsed += remaining[index]
            remaining[index] = 0
            fmt.Fprintf(w, "%d %s finish\n", elapsed, processes[index].Name)
        }
    }
}

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
