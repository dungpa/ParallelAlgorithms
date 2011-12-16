#r "FSharp.Powerpack.Parallel.Seq.dll"
#load "Primes.fs"

open ParallelAlgorithms.Primes

let N = 10000000;;
#time "on";;
let a = N |> primesUnder;;
let b = N |> primesUnderParallel;;
//
//// Shouldn't use with big N.
//let c = N |> primesUnderSeq |> Seq.toArray;;
//let d = N |> primesUnderPSeq |> Seq.toArray;;
//let e = N |> primesUnderSeq2 |> Seq.toArray;;
//let f = N |> primesUnderPSeq2 |> Seq.toArray;;
