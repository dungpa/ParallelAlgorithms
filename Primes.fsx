#load "Primes.fs"

open ParallelAlgorithms.Primes

#time "on";;
let _ = primesUnder 10000000;;
let _ = pprimesUnder 10000000;;


