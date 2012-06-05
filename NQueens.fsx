#load "NQueens.fs"

open ParallelAlgorithms.NQueens

#time "on";;

let size = 14;;

let c1 = countSequential1 size;;
let c2 = countParallel1 size;;

let c3 = countSequential2 size;;
let c4 = countParallel2 size;;