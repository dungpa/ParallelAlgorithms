#r "FSharp.Powerpack.Parallel.Seq.dll"
#load "Mersenne.fs"

open ParallelAlgorithms.Mersenne

let N = 2048
#time "on";;

let m1 = runPerfectsSeq N;;
let m2 = runPerfectsPSeq N;;

let m3 = runPerfects N;;
let m4 = runPerfectsPar N;;
let m5 = runPerfectsPar2 N;;