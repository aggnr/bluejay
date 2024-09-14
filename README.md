![Alt text](bluejay.ico)
# BlueJay
BlueJay is a framework that provides a simple interface for data analysis.

## Installation
To install the cow package, use the following command:
    
```
go get github.com/aggnr/cow
```

## Usage

### Creating a data frame
You can create a data frame using the `NewDataFrame` function. The function takes a map of column names to slices of values as input.

``` 
package main

import (
	"fmt"
	"github.com/aggnr/cow"
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

	df, err := cow.NewDataFrame(people)
	if err != nil {
		log.Fatalf("Error creating DataFrame: %v", err)
	}
	defer df.Close()

	fmt.Println("DataFrame created successfully!")

}
```

### More examples
For more examples, see the [examples](examples) directory.

# License
This project is licensed under the MIT License. See the LICENSE file for details.
