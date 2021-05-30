# rsort

[![Build Status](https://travis-ci.com/XiaochenCui/rsort.svg?branch=master)](https://travis-ci.com/XiaochenCui/rsort)

A substitute of gnu sort.
Use external merge sort to sort arbitrarily large files, and ensure that the memory allocation does not exceed 1GB.

## TODO
- Thread pool
- Secondary cache
- 1T sort test
- Unittest memory report on Linux
- Unittest memory report on MacOS