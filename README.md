
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

### Basic Usage

- [hellogf](examples/hellogf.go) - This example demonstrates how to create a simple DataFrame from a slice of structs.
- [display](examples/display.go) - This example demonstrates how to display the contents of a DataFrame.
- [head](examples/head.go) - This example demonstrates how to display the first few rows of a DataFrame.
- [info](examples/info.go) - This example demonstrates how to display summary information about a DataFrame.
- [loc](examples/loc.go) - This example demonstrates how to select specific rows and columns from a DataFrame.
- [readcsv](examples/readcsv.go) - This example demonstrates how to read data from a CSV file into a DataFrame.
- [readjson](examples/readjson.go) - This example demonstrates how to read data from a JSON file into a DataFrame.
- [tail](examples/tail.go) - This example demonstrates how to display the last few rows of a DataFrame.

### Advanced Usage

- [corr](examples/corr.go) - This example demonstrates how to use the Corr method to calculate the correlation matrix of a DataFrame.

# License
This project is licensed under the MIT License. See the LICENSE file for details.
