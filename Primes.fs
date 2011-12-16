﻿module ParallelAlgorithms.Primes

// A simple parallel version of prime sieves.

// Some lessons learned:
// 1. Functions which are passed as lambda should be inlined (a common optimization)
// 2. Avoid creating unnecessary data (using filterRange instead of Array.filter)

open System
open System.Collections.Concurrent
open System.Threading.Tasks

open Microsoft.FSharp.Collections

let inline indivisible divisors b =
    Seq.forall (fun a -> b%a<>0) divisors

// divides and anyP functions are kept for future reference
// The speedup could be 6x if divides is not inline
let divides a b = b % a = 0

let anyP b smallers =
    Array.exists (fun a -> divides b a) smallers
//

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
    | n ->  let ns = n |> float |> sqrt |> ceil |> int
            let smallers = primesUnder ns
            Array.append smallers (filterRange (indivisible smallers) (ns, n-1))

let rec primesUnderParallel = function
    | n when n<=2 ->  [||]
    | 3 ->  [|2|]
    | n when n<=100 -> primesUnder n
    | n ->  let ns = n |> float |> sqrt |> ceil |> int
            let smallers = primesUnderParallel ns            
            Array.append smallers (pfilterRange (indivisible smallers) (ns, n-1))

// Seq versions are more readable but wasted because of using lots of extra sequences.
let primesUnderSeq n = 
    let rec loop n = 
        seq {
            match n with
            | _ when n <= 2 -> ()
            | 3 -> yield 2
            | _ -> let ns = n |> float |> sqrt |> ceil |> int
                   let smallers = loop ns
                   yield! smallers
                   yield! seq {ns..n-1} |> Seq.filter (indivisible smallers)
        }
    loop n

let primesUnderPSeq n = 
    let rec loop n = 
        seq {
            match n with
            | _ when n <= 2 -> ()
            | 3 -> yield 2
            | _ when n<=100 -> yield! primesUnderSeq n
            | _ -> let ns = n |> float |> sqrt |> ceil |> int
                   let smallers = loop ns
                   yield! smallers
                   yield! seq {ns..n-1} |> PSeq.filter (indivisible smallers)
        }
    loop n

// Inefficient versions using Seq
let primesUnderSeq2 n = 
    let rec loop = function             
        | n when n <= 2 -> Seq.empty
        | 3 -> Seq.singleton 2        
        | n ->  let ns = n |> float |> sqrt |> ceil |> int
                let smallers = loop ns
                seq {ns..n-1} |> Seq.filter (indivisible smallers) |> Seq.append smallers 
    loop n

let primesUnderPSeq2 n = 
    let rec loop = function             
        | n when n <= 2 -> Seq.empty
        | 3 -> Seq.singleton 2
        | n when n<=100 -> primesUnderSeq2 n
        | n ->  let ns = n |> float |> sqrt |> ceil |> int
                let smallers = loop ns
                seq {ns..n-1} |> PSeq.filter (indivisible smallers) |> Seq.append smallers 
    loop n