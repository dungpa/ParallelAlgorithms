module ParallelAlgorithms.Program

open System
open System.Diagnostics

open Geonames

type Mode = LINQ | PLINQ | SEQUENTIAL | PARALLEL

let run (inFile, degreeOfParallelism, mode) =
    let watch = new Stopwatch()
    watch.Start()
    match mode with
    | LINQ ->
             printfn "Mode LINQ"
             inFile |> searchLINQ |> display
    | PLINQ -> 
            printfn "Mode PLINQ"
            (inFile, degreeOfParallelism) |> searchPLINQ |> display            
    | SEQUENTIAL -> 
            printfn "Mode Serial"
            inFile |> searchSequential |> display
    | PARALLEL -> 
            printfn "Mode Parallel"
            (inFile, degreeOfParallelism) |> searchParallel |> display
    Console.WriteLine("Total Elapsed time: {0}.{1} seconds.", watch.ElapsedMilliseconds / 1000L, watch.ElapsedMilliseconds % 1000L)
    watch.Stop()

run("Data/allcountries.txt", 10, PLINQ)
Console.ReadKey() |> ignore
