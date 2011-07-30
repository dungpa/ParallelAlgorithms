#load "Strassens.fs"

open ParallelAlgorithms.Strassens

#time "on";;

let size = 1024;;

let A = Matrix(size)
A.fillRandom();;

let B = Matrix(size)
B.fillRandom();;

let C1 = Matrix.multiply(A, B);;
let C2 = Matrix.multiplyParallel(A, B);;

let C3 = Matrix.multiplyStrassen(A, B);;
let C4 = Matrix.multiplyStrassenParallel(A, B);;