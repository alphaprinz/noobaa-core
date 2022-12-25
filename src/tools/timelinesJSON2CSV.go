package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type Timestamp struct {
	Name string
	Ts   string
}

type Timeline struct {
	Name string
	Line []Timestamp
}

func main() {
	jsonFile := os.Stdin
	if len(os.Args) <= 1 {
		fmt.Fprintln(os.Stderr, "Missing file name argument, reading from stdint")
	} else {
		jsonFileName := os.Args[1]
		fmt.Fprintln(os.Stderr, "Reading from file", jsonFileName)
		var err error
		jsonFile, err = os.Open(jsonFileName)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer jsonFile.Close()
	}
	jsonData, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		fmt.Println(err)
		return
	}
	var timelines []Timeline
	err = json.Unmarshal(jsonData, &timelines)
	if err != nil {
		fmt.Println(err)
		return
	}

	printCSV(timelines)
}

func calcColumns(timelines []Timeline) []string {
	waypoints := []string{}
	waypointNames := map[string]bool{}

	for _, tl := range timelines {
		wi := 0
		for i, _ := range tl.Line {
			_, exists := waypointNames[tl.Line[i].Name]
			if !exists {
				waypointNames[tl.Line[i].Name] = true
				copyWaypoints := make([]string, len(waypoints))
				copy(copyWaypoints, waypoints)
				headSlice := append(waypoints[0:wi], tl.Line[i].Name)
				tailSlice := copyWaypoints[wi:(len(copyWaypoints))]
				waypoints = append(headSlice, tailSlice...)
			} else {
				for ; tl.Line[i].Name != waypoints[wi]; wi++ {
				}
			}
			wi += 1
		}
	}

	return waypoints
}

func printCSV(timelines []Timeline) {
	cols := calcColumns(timelines)
	// header
	printHeader(cols)

	for _, tl := range timelines {
		printLine(cols, tl)
	}
}

func printHeader(cols []string) {
	fmt.Fprintln(os.Stderr, "Headers", cols)
	firstHeader := true
	for _, c := range cols {
		if firstHeader {
			firstHeader = false
		} else {
			fmt.Printf(", ")
		}
		fmt.Printf("%s", c)
	}
	fmt.Printf("\n")
}

func printLine(cols []string, tl Timeline) {
	firstCol := true
	i := 0
	fmt.Fprintln(os.Stderr, "Line", tl.Line)
	for _, col := range cols {
		if firstCol {
			firstCol = false
		} else {
			fmt.Printf(", ")
		}
		if i < len(tl.Line) && tl.Line[i].Name == col {
			fmt.Printf("%s", tl.Line[i].Ts)
			i++
		} else {
			fmt.Printf("NaN")
		}
	}
	fmt.Printf("\n")
}
