namespace ParallelAlgorithms

// A simple parallel version of prime sieves.

// Some lessons learned:
// 1. Functions which are passed as lambda should be inlined (a common optimization)
// 2. Avoid creating unnecessary data (using filterRange instead of Array.filter)

module Primes =

    open System

    open System.Collections.Concurrent
    open System.Threading.Tasks

    let inline indivisible b divisors =
        Array.forall (fun a -> b%a<>0) divisors

    // divides and anyP functions are kept for reference
    // The speedup could be 6x if divides is not inline
    let divides a b = b % a = 0

    let anyP b smallers =
        Array.exists (fun a -> divides b a) smallers

    let filterRange predicate (i, j) =
        let results = new ResizeArray<int>(j-i+1) // reserve quite a lot of space.
        for k = i to j do
            if predicate k then results.Add(k)
        results.ToArray()
  
    let pfilterRange predicate (i, j) =
        let results = new ResizeArray<int>(j-i+1)
        let monitor = new Object()
        Parallel.For(
            i, j, new ParallelOptions(),
            (fun () -> new ResizeArray<int>(j-i+1)), 
            (fun k _ (localList: ResizeArray<int>) ->                          
                if predicate k then localList.Add(k)
                localList),
            (fun local -> lock (monitor) (fun () -> results.AddRange(local)))) |> ignore
        results.ToArray()

    let rec primesUnder = function
        | n when n<=2 -> [||]
        | 3 ->  [|2|]
        | n ->  let ns = int (Math.Ceiling(sqrt(float n)))
                let smallers = primesUnder ns
                Array.append smallers (filterRange (fun i -> indivisible i smallers) (ns, n-1))

    let rec pprimesUnder = function
        | n when n<=2 ->  [||]
        | 3 ->  [|2|]
        | n when n<=100 -> primesUnder n
        | n ->  let ns = int (Math.Ceiling(sqrt(float n)))
                let smallers = pprimesUnder ns            
                Array.append smallers (pfilterRange (fun i -> indivisible i smallers) (ns, n-1))



