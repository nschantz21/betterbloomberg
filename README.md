# betterbloomberg

A wrapper for the [Bloomberg Desktop Python API](https://www.bloomberg.com/professional/support/api-library/)  

The goal is to create a lite wrapper for the blpapi package for a more pythonic interface.  
Also will attempt to make more comprehensive and documentation, but there already exists the official Bloomberg API documentation.  
This will also be heavily dependent on the pandas package.  

It would be better to expose the C or C++ api through Cython, but this will do for now.  

## BLP API Services to Wrap  

### Reference Data  
* Static - snapshot of the current value  
* Historical End-of-Day  
* ~~Historical Intraday Bar/Tick~~ - Not Authorized
* Portfolio
* Equity Screen  

### VWAP  

### Market Data  
probably not worth it.

#### Market List

#### Market Bar  

### Field 
* Field Search  
* Field Information  
* ~~Field List~~  - these are pretty much covered by the field search
* ~~Categorized Field Search~~  

### Instrument Search 
* Security
* Curve
* Government

### Page Data  
probably not worth it  

### Technical Analysis  



I think there's also a way to emulate the search bar -- I'll look into that

