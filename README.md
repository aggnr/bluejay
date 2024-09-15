
<div align="center">
  <img src="bluejay.ico" alt="BlueJay Icon">
</div>


<p align="center">
  <a href="https://github.com/aggnr/bluejay/releases/latest">
    <img src="https://img.shields.io/github/v/tag/aggnr/bluejay?label=latest%20release" alt="Latest Release">
  </a>
  <a href="https://github.com/aggnr/bluejay/blob/main/LICENSE">
    <img src="https://img.shields.io/github/license/aggnr/bluejay" alt="License">
  </a>
    
</p>


# BlueJay
BlueJay is a framework that provides a simple interface for data analysis.

## Installation
To install the bluejay package, use the following command:
    
```
go get github.com/aggnr/bluejay
```

## Usage

### Creating a data frame
You can create a data frame using the `NewDataFrame` function. The function takes a map of column names to slices of values as input.

``` 
package main

import (
	"fmt"
	"github.com/aggnr/bluejay"
	"log"
)

func main() {

	type Person struct {
		Name      string
		Age       int
		Salary    float64
		IsMarried bool
	}

	people := []Person{
		{"John", 30, 50000.50, true},
		{"Jane", 25, 60000.75, false},
	}

	df, err := bluejay.NewDataFrame(people)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}
	defer df.Close()

	fmt.Println("DataFrame created successfully!")

}
```

## Examples

Here are some examples demonstrating how to use BlueJay:

- [display](examples/display.go)
- [head](examples/head.go)
- [hellogf](examples/hellogf.go)
- [info](examples/info.go)
- [loc](examples/loc.go)
- [readcsv](examples/readcsv.go)
- [readjson](examples/readjson.go)
- [tail](examples/tail.go)

# License
This project is licensed under the MIT License. See the LICENSE file for details.
