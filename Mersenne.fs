module ParallelAlgorithms.Mersenne
// The original question is at http://stackoverflow.com/questions/8368107/f-parallelizing-issue-when-calculating-perfect-numbers/
// The idea is searching for perfect numbers in a parallel manner.

open System
open System.Threading.Tasks
open System.Collections.Concurrent

open Microsoft.FSharp.Collections

let inline PowShift exp = 1I <<< exp

// Source at http://stackoverflow.com/questions/2053691/can-the-execution-time-of-this-prime-number-generator-be-improved
let isPrime(n: bigint) = 
    let maxFactor = bigint(sqrt(float n))
    let rec loop testPrime tog =
        if testPrime > maxFactor then true
        elif n % testPrime = 0I then false
        else loop (testPrime + tog) (6I - tog)
    if n = 2I || n = 3I || n = 5I then true
    elif n <= 1I || n % 2I = 0I || n % 3I = 0I || n % 5I = 0I then false
    else loop 7I 4I

// The Lucas-Lehmer primality test for Mersenne numbers
// Reference at http://en.wikipedia.org/wiki/Lucas%E2%80%93Lehmer_primality_test
let lucasLehmer p =
    let m = (PowShift p) - 1I
    let rec loop i acc =
        if i = p-2 then acc
        else loop (i+1) ((acc*acc - 2I)%m)
    (loop 0 4I) = 0I

// Solutions using Seq and PSeq
let inline mersenne (i: int) =     
    if i = 2 || (isPrime (bigint i) && lucasLehmer i) then
        let p = PowShift i
        Some (i, (p/2I) * (p-1I))
    else None

let runPerfectsSeq n =
    seq {1..n}
        |> Seq.choose mersenne
        |> Seq.toArray
    
let runPerfectsPSeq n =
    seq {1..n}
        |> PSeq.choose mersenne
        |> PSeq.sort (* align with sequential version *)
        |> PSeq.toArray 

// Solutions using for and Parallel.For
let runPerfects(n: int) =
    let results = new ResizeArray<_>(n)
    for i in 1..n do
        if i = 2 || (isPrime (bigint i) && lucasLehmer i) then
            let p = PowShift i
            results.Add(i, (p/2I) * (p-1I))
    results.ToArray()
    
let runPerfectsPar(n: int) =
    let results = new ResizeArray<_>(n)
    let monitor = new Object()
    Parallel.For(
        1, n+1, new ParallelOptions(),
        (fun () -> new ResizeArray<_>(n)), 
        (fun i _ (localList: ResizeArray<_>) ->                          
            if i = 2 || (isPrime (bigint i) && lucasLehmer i) then
                let p = PowShift i
                localList.Add(i, (p/2I) * (p-1I))
            localList),
        (fun local -> lock (monitor) (fun () -> results.AddRange(local)))) |> ignore
    results.Sort()
    results.ToArray()

// More complex, but just slightly better than runPerfectsPar
let runPerfectsPar2(n: int) =
    let size = Environment.ProcessorCount * 256
    let rangeSize = n / size
    let partitions = Partitioner.Create(0, n, if rangeSize >= 1 then rangeSize else 1)
    let output = Array.zeroCreate size

    Parallel.ForEach(
        partitions, new ParallelOptions(), 
        (fun (min, max) _ ->     
            let localList = new ResizeArray<_>(n)
            for i in min+1..max do                     
                if i = 2 || (isPrime (bigint i) && lucasLehmer i) then
                    let p = PowShift i
                    localList.Add(i, (p/2I) * (p-1I))
            output.[min/rangeSize] <- localList)) |> ignore

    output |> Array.collect(fun ra -> ra.ToArray()) |> Array.sort