package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/hannahhoward/go-storethehash"
	"github.com/ipld/go-car"
	"gopkg.in/cheggaaa/pb.v1"
)

func main() {
	params := os.Args[1:]
	if len(params) != 3 {
		fmt.Println("incorrect arguments")
		os.Exit(1)
	}
	bs, err := storethehash.OpenHashedBlockstore(params[0], params[1])
	if err != nil {
		fmt.Printf("error opening blockstore: %s\n", err.Error())
		os.Exit(1)
	}

	bs.Start()

	reader, err := os.Open(params[2])
	if err != nil {
		fmt.Printf("error opening CAR file: %s\n", err.Error())
		os.Exit(1)
	}

	stat, err := reader.Stat()
	if err != nil {
		fmt.Printf("error sizing CAR file: %s\n", err.Error())
		os.Exit(1)
	}

	bufr := bufio.NewReaderSize(reader, 1<<20)

	bar := pb.New64(stat.Size())
	br := bar.NewProxyReader(bufr)
	bar.ShowTimeLeft = true
	bar.ShowPercent = true
	bar.ShowSpeed = true
	bar.Units = pb.U_BYTES

	bar.Start()
	_, err = car.LoadCar(bs, br)
	bar.Finish()

	if err != nil {
		fmt.Printf("Error loading car file to blockstore: %s\n", err.Error())
		os.Exit(1)
	}

	bs.Close()
}
