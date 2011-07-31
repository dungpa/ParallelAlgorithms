#load "Strassens.fs"

// Limitations:
// 1. Strassen's algorithm only works with power-of-two sizes.
// 2. Strassen's algorithm uses GCHandle; the system could run out of memory with huge matrices.

open ParallelAlgorithms.Strassens

#time "on";;

let size = 1024;;

let A = initRandomMatrix(size);;
let B = initRandomMatrix(size);;

let C1 = multiply(A, B);;
let C2 = multiplyParallel(A, B);;

let C3 = multiplyStrassen(A, B);;
let C4 = multiplyStrassenParallel(A, B);;